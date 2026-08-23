# План Этапа 3 — Circuit breaker (ядро)

Ветка: `stage-3-circuit-breaker` (от `master`, HEAD `b494576`).
Исполнитель: кодер `model: opus` по этому файлу. **Не коммитить** — коммит/PR
делает основная сессия по явной команде пользователя.

Ссылки: [TECHNICAL_PLAN.md](../TECHNICAL_PLAN.md) разделы «Конкурентная модель
circuit breaker» и «Этап 3»; [SKILL.md](../../.claude/skills/go-circuitproxy-dev/SKILL.md)
разделы «Конкурентная модель» и «Паттерн тестирования конкурентности».

## Цель

Реализовать конкурентно-корректный per-backend circuit breaker (`internal/breaker`)
и интегрировать его в reverse proxy (`internal/proxy`): перед проксированием —
`Allow()`, после — `Report(success)`. Публичный API breaker'а уже зафиксирован в
сигнатурах (`New`, `Allow`, `Report`, `State`) — реализовать только тела.

## Критерий готовности (DoD)

- `internal/breaker`: автомат closed→open→half-open→closed реализован по алгоритму
  ниже; «ровно один пробник» в half-open доказан конкурентным тестом под `-race`.
- `internal/proxy`: breaker на каждый backend; при `open`/half-open-loser запросы
  fast-fail без реального обращения к backend'у; исход каждого запроса докладывается
  breaker'у корректному backend'у.
- `internal/config`: у `BreakerConfig` есть метод `OpenTimeout() time.Duration`
  (JSON-схема не меняется).
- `gofmt -l .` пусто, `go vet ./...` и `go test -race ./...` (в т.ч. `-count=10` на
  breaker-пакете) зелёные.
- Retry (Этап 4) НЕ реализован; `internal/healthcheck` не тронут; схема
  `config.Config` (JSON-поля) не менялась.

---

## Архитектурные решения (приняты, реализовать как написано)

### Решение 1 — где живёт `*breaker.Breaker` каждого backend'а

Breaker хранится **полем в `proxy.Backend`** (неэкспортируемое, как `up`),
конструируется в `NewBalancer` из `cfg.Breaker`. Доступ — через **тонкие
экспортируемые методы-обёртки** `Backend.Allow()` / `Backend.Report(bool)`, по
аналогии с уже принятым на Этапе 2 паттерном `IsUp()/SetUp()`. Само поле-указатель
наружу не отдаём.

Обоснование:
- Симметрия с уже согласованным решением Этапа 2 (handoff stage-2, «вариант (a)»):
  неэкспортируемое поле + методы-обёртки. Не плодим второй стиль доступа.
- Инкапсуляция: вызывающий код (`Handler()`, будущий retry-`RoundTripper` Этапа 4)
  работает с `Backend`, ему не нужен тип `*breaker.Breaker` напрямую.
- **Breaker НЕ нужен health-checker'у.** Health-check (liveness, `IsUp/SetUp`) и
  circuit breaker — независимые механизмы (TECHNICAL_PLAN: backend может быть `up`
  по health-check, но circuit `open` по недавним ошибкам, и наоборот). Поэтому
  `Backend.Allow/Report` НЕ добавляются в `Balancer.Backends()`-контракт для
  healthcheck и не путаются с liveness. `internal/healthcheck` не трогаем вообще.

### Решение 2 — как `Allow()`/`Report()` хукаются в общий `*httputil.ReverseProxy`

Сохраняем архитектуру Этапа 1/2: один переиспользуемый `*httputil.ReverseProxy`,
backend выбирается во внешнем `http.HandlerFunc` и передаётся в хуки через
`context.WithValue(backendCtxKey{})` (как сейчас в `Rewrite`/`ErrorHandler`).

**`Allow()` — во внешнем handler'е, сразу после `Next()`, до `rp.ServeHTTP`.**
Это единственное место, где выбор backend'а уже сделан, но реального обращения ещё
не было — естественная точка fast-fail. Если `Allow()` вернул `false`
(`ErrBreakerOpen`) — отвечаем сразу, `rp.ServeHTTP` не вызываем (backend не получает
HTTP-запрос).

**HTTP-статус при `ErrBreakerOpen`: `503 Service Unavailable`.** Обоснование:
- Прецедент «нет backend'ов» уже отдаёт `503` (`http.Error(..., StatusServiceUnavailable)`).
  Открытый circuit концептуально того же рода — «backend сейчас недоступен», просто
  по решению breaker'а, а не из-за пустого пула. Держим единый семантический класс
  «сервис временно недоступен» = 503.
- `503` — стандартный ответ «попробуй позже» (в отличие от `502 Bad Gateway`,
  который означает «сходил на backend и получил плохой ответ» — здесь мы намеренно
  НЕ ходили). `502` оставляем строго за реальной транспортной ошибкой в
  `ErrorHandler`.
- Тело: `http.Error(w, "circuit breaker open", http.StatusServiceUnavailable)` —
  отличимо в тесте от «no backends available» по тексту, оба 503.

**`Report(success)` — через два хука ReverseProxy, каждый берёт backend из
context'а того же запроса:**

1. **`ErrorHandler`** (уже есть) — вызывается ТОЛЬКО при ошибке транспорта/RoundTrip
   (подтверждено сверкой с `$GOROOT/src/net/http/httputil/reverseproxy.go`: `ErrorHandler`
   вызывается на ошибке `RoundTrip` до `WriteHeader`; ошибки копирования тела после
   заголовков идут через `panic(http.ErrAbortHandler)`, не сюда — см. handoff Этапа 1).
   Транспортная ошибка = **неуспех** → в конце `ErrorHandler` вызвать
   `backend.Report(false)` (backend уже достаётся из context'а — не менять этот код,
   добавить один вызов).

2. **`ModifyResponse func(*http.Response) error`** (СЕЙЧАС ОТСУТСТВУЕТ — завести).
   Вызывается на успешном транспортном приёме ответа, ДО записи клиенту. Здесь
   классифицируем ответ по статус-коду (правило — Решение 3) и вызываем
   `backend.Report(успех)`. Backend берём из `res.Request.Context()` (у ответа есть
   `Request` с тем же context'ом, что нёс `backendCtxKey`).

   **КРИТИЧНО — `ModifyResponse` ВСЕГДА возвращает `nil`, даже для 5xx.** Причина
   (сверено с stdlib, `reverseproxy.go:189-192, 501-503`): если `ModifyResponse`
   вернёт не-`nil` ошибку, ReverseProxy вызовет `ErrorHandler` с этой ошибкой — и
   тогда `Report(false)` сработает ДВАЖДЫ (один раз в `ModifyResponse`, второй в
   `ErrorHandler`), а клиент вместо 5xx-ответа backend'а получит наш 502. Мы НЕ
   хотим ни двойного отчёта, ни подмены ответа: 5xx-ответ backend'а должен дойти до
   клиента как есть, breaker лишь фиксирует его как неуспех. Поэтому `ModifyResponse`
   только читает `res.StatusCode`, зовёт `Report(...)` и `return nil`.

Итог: каждый запрос докладывает breaker'у **ровно один раз** — либо через
`ModifyResponse` (транспорт успешен, есть ответ с кодом), либо через `ErrorHandler`
(транспорт упал). Оба хука знают свой `*proxy.Backend` из context'а запроса.

### Решение 3 — критерий успех/неуспех для `Report()`

- **Транспортная ошибка** (путь `ErrorHandler`) → **неуспех** (`Report(false)`).
- **Ответ получен** (путь `ModifyResponse`):
  - `res.StatusCode >= 500` → **неуспех** (`Report(false)`) — 5xx сигнализирует о
    проблеме самого backend'а.
  - `res.StatusCode < 500` (1xx/2xx/3xx/4xx) → **успех** (`Report(true)`) — backend
    ответил; 4xx — вина клиента/запроса, не признак нездоровья backend'а.

Стандартная практика circuit breaker. Зафиксировано явно; порог = 500.
Реализовать как helper во внешнем коде хука, напр. `success := res.StatusCode < 500`.

### Решение 4 — `config.BreakerConfig.OpenTimeout()`

Добавить в `internal/config/config.go` метод-конвертер рядом с
`HealthCheckConfig.Interval()/.Timeout()`:

```go
// OpenTimeout returns the breaker open-state timeout as a time.Duration.
func (b BreakerConfig) OpenTimeout() time.Duration {
	return time.Duration(b.OpenTimeoutSeconds) * time.Second
}
```

JSON-схему (`BreakerConfig` поля/теги) НЕ менять. `duration`-тип уже существует.

### Решение 5 — не блокировать half-open-неудачников

`Allow()` НЕ блокирует и не ждёт результата пробника (стратегия fast-fail из
TECHNICAL_PLAN уже принята). Проигравшие `trialInFlight`-CAS получают
`(false, ErrBreakerOpen)` немедленно. Интеграция в proxy это уважает автоматически:
`Allow()` вызывается синхронно во внешнем handler'е и на `false` сразу отдаёт 503,
никаких очередей/ожидающих горутин не заводится.

---

## Изменения по файлам

### `internal/breaker/breaker.go` — реализовать тела `Allow` и `Report`

Сигнатуры и структура `Breaker`/`Config`/`New` уже есть, НЕ менять. Реализовать:

**`func (b *Breaker) Allow() (bool, error)`** — алгоритм:

```
state := State(b.state.Load())
switch state {
case StateClosed:
    return true, nil
case StateOpen:
    // истёк ли OpenTimeout?
    openedAt := b.openedAtNanos.Load()
    if b.now().UnixNano()-openedAt < int64(b.openTimeout) {
        return false, ErrBreakerOpen        // ещё рано — fast-fail
    }
    // пора пробовать: CAS open->half-open
    if b.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
        // победитель перевода продолжает в half-open-ветку ТОЙ ЖЕ вызова
        // (провалиться вниз в допуск пробника)
    }
    // и победитель, и проигравшие CAS теперь идут в half-open-допуск ниже
    fallthrough  // ИЛИ явный переход в half-open-ветку — см. примечание
case StateHalfOpen:
    if b.trialInFlight.CompareAndSwap(false, true) {
        return true, nil                     // ЕДИНСТВЕННЫЙ пробник
    }
    return false, ErrBreakerOpen             // остальные — fast-fail
}
return false, ErrBreakerOpen                 // недостижимо, защитный дефолт
```

Примечание по управлению потоком: Go `fallthrough` из `case StateOpen` перепрыгнет
в тело `case StateHalfOpen` безусловно, но только для той горутины, что дошла до
конца `case StateOpen` (т.е. после проверки таймаута). Это корректно: и победитель
перевода, и проигравшие (кто увидел уже-half-open) должны попасть в CAS
`trialInFlight`. Если `fallthrough` из-под `if` читается плохо — допустимо вынести
half-open-допуск в приватный метод `admitHalfOpenProbe() (bool, error)` и звать его
из обеих веток; выбрать один вариант, не смешивать. **Важно:** проигравшие CAS
`state` НЕ должны делать ретрай/спин — они просто идут в `trialInFlight`-CAS, где
почти наверняка проиграют (пробник уже взят победителем), и получат fast-fail. Это
и есть желаемое поведение.

**`func (b *Breaker) Report(success bool)`** — алгоритм:

```
state := State(b.state.Load())
switch state {
case StateHalfOpen:
    if success {
        b.failures.Store(0)
        b.state.Store(int32(StateClosed))
        b.trialInFlight.Store(false)      // сбросить последним: сначала closed, потом отпустить флаг
    } else {
        b.openedAtNanos.Store(b.now().UnixNano())
        b.state.Store(int32(StateOpen))
        b.trialInFlight.Store(false)
    }
case StateClosed:
    if success {
        // сброс серии, только если она есть (избегаем лишней записи)
        if b.failures.Load() > 0 {
            b.failures.Store(0)
        }
    } else {
        n := b.failures.Add(1)
        threshold := b.failureThreshold
        if threshold <= 0 {
            threshold = 1                  // см. граничный случай ниже
        }
        if n >= threshold {
            // перейти в open; CAS, чтобы не перезатереть чужой переход
            if b.state.CompareAndSwap(int32(StateClosed), int32(StateOpen)) {
                b.openedAtNanos.Store(b.now().UnixNano())
            }
        }
    }
case StateOpen:
    // защитный no-op — см. граничный случай ниже
}
```

Порядок записей в half-open важен: сбрасывать `trialInFlight` **последним**, уже
после установки целевого `state`. Иначе между отпусканием флага и сменой состояния
другая горутина могла бы увидеть half-open с `trialInFlight=false` и взять «второй»
пробник в том же окне. (В строгом смысле atomic-поля независимы и полной линеаризации
это не даёт, но для MVP-контракта «ровно один пробник в окне до Report» достаточно;
конкурентный тест проверяет именно окно open→half-open до первого Report — там
`trialInFlight` ещё не сбрасывался.)

**Граничные случаи (реализовать и покрыть тестом/комментарием):**

- **`Report(false)` при state уже `StateOpen`.** При корректной интеграции недостижимо
  (в open запросы fast-fail в `Allow()`, до backend'а не доходят, `Report` не
  вызывается). Защитное поведение: **no-op** (ветка `case StateOpen` пустая). НЕ
  трогать `openedAtNanos` — иначе гонка могла бы бесконечно продлевать open-окно.
  Оставить поясняющий комментарий.
- **`FailureThreshold <= 0` в конфиге.** Конфиг не валидируется до Этапа 5 (та же
  природа, что `interval_seconds=0` на Этапе 2). Трактовать `<= 0` как порог `1`
  («открываться после первой же ошибки»), а не паниковать. Деления на ноль в коде
  нет; фикс — локальная нормализация `threshold` в `Report` (см. псевдокод) ИЛИ
  один раз в `New` (нормализовать `b.failureThreshold` при построении — предпочтительно,
  чтобы не повторять проверку на каждый `Report`). **Выбрать нормализацию в `New`**:
  в конструкторе после копирования полей `if b.failureThreshold <= 0 { b.failureThreshold = 1 }`;
  тогда в `Report` порог уже валиден. Оставить комментарий про Этап 5.

**Тестовый хелпер инъекции времени.** Использовать вариант из SKILL.md — приватный
метод в `_test.go` того же пакета:

```go
// breaker_test.go
func (b *Breaker) forceOpenedAt(t time.Time) {
	b.openedAtNanos.Store(t.UnixNano())
}
```

Он переводит breaker в состояние «время открытия давно прошло» без real sleep:
`b.forceOpenedAt(time.Now().Add(-time.Hour))` при `state==open` заставит следующий
`Allow()` пройти CAS open→half-open. Для тестов, управляющих временем «вперёд»,
допустимо также сконструировать `Breaker` с `Config.Now`, возвращающим управляемое
тестом время (напр. через `atomic.Int64`-обёртку) — но для Этапа 3 хватает
`forceOpenedAt`; выбран он, второй способ не заводить без нужды.

### `internal/config/config.go` — добавить метод

Добавить `func (b BreakerConfig) OpenTimeout() time.Duration` (см. Решение 4) рядом
с `HealthCheckConfig.Interval()/.Timeout()`. Больше ничего в config.go не менять.

### `internal/proxy/proxy.go` — интеграция breaker

1. **Импорт** `"github.com/akomyagin/circuitproxy/internal/breaker"`.

2. **Поле в `Backend`:**
   ```go
   type Backend struct {
       URL *url.URL
       up  atomic.Bool
       cb  *breaker.Breaker   // per-backend circuit breaker (Этап 3)
   }
   ```

3. **Методы-обёртки** (рядом с `IsUp/SetUp`):
   ```go
   // Allow reports whether the circuit breaker permits a request to this
   // backend right now. See internal/breaker.
   func (b *Backend) Allow() (bool, error) { return b.cb.Allow() }

   // Report records the outcome (success/failure) of a request to this backend
   // to its circuit breaker.
   func (b *Backend) Report(success bool) { b.cb.Report(success) }
   ```

4. **Конструирование в `NewBalancer`.** В цикле по backend'ам, после `b := &Backend{URL: u}`:
   ```go
   b.cb = breaker.New(breaker.Config{
       FailureThreshold: cfg.Breaker.FailureThreshold,
       OpenTimeout:      cfg.Breaker.OpenTimeout(),
       // Now не задаём — breaker.New дефолтит на time.Now.
   })
   ```
   `cfg` уже `*config.Config`, поле `cfg.Breaker` доступно. Тесты, которым нужна
   инъекция времени в breaker через proxy, сконструируют backend/breaker напрямую
   или через тестовый хелпер (см. тест-кейсы) — но `NewBalancer` в проде берёт
   `time.Now`.

5. **`Handler()` — вызвать `Allow()` перед проксированием.** Во внешнем
   `http.HandlerFunc`, после успешного `Next()`:
   ```go
   backend := b.Next()
   if backend == nil {
       http.Error(w, "no backends available", http.StatusServiceUnavailable)
       return
   }
   if ok, _ := backend.Allow(); !ok {
       http.Error(w, "circuit breaker open", http.StatusServiceUnavailable)
       return
   }
   ctx := context.WithValue(r.Context(), backendCtxKey{}, backend)
   rp.ServeHTTP(w, r.WithContext(ctx))
   ```
   (Ошибку `Allow()` игнорируем — она всегда `ErrBreakerOpen` при `false`; статус
   один и тот же 503.)

6. **`ModifyResponse` — завести хук, докладывать исход по статусу.** В литерале
   `&httputil.ReverseProxy{...}` добавить рядом с `Rewrite`/`ErrorHandler`:
   ```go
   ModifyResponse: func(res *http.Response) error {
       // res.Request carries the same context that Rewrite/ErrorHandler use.
       backend, _ := res.Request.Context().Value(backendCtxKey{}).(*Backend)
       if backend != nil {
           backend.Report(res.StatusCode < 500)
       }
       // ALWAYS return nil: returning an error would (a) route this response
       // through ErrorHandler (double Report) and (b) replace the backend's
       // response with our 502. A 5xx from the backend must reach the client
       // unchanged; the breaker only records it. See stdlib reverseproxy.go
       // (ModifyResponse error -> ErrorHandler).
       return nil
   },
   ```

7. **`ErrorHandler` — добавить `Report(false)`.** В существующем `ErrorHandler`,
   после логирования, перед/после `w.WriteHeader(http.StatusBadGateway)` (порядок
   некритичен, но логичнее до записи ответа):
   ```go
   if backend != nil {
       backend.Report(false)   // transport error counts as a failure
   }
   ```
   `backend` в `ErrorHandler` уже достаётся из context'а (не менять эту строку).

8. **Актуализировать докстринг `Handler()`:** убрать `TODO(Этап 3)` (реализовано),
   оставить `TODO(Этап 4)` про retry. Кратко описать, что `Allow()` вызывается до
   проксирования (fast-fail 503 на open), `Report` — через `ModifyResponse`
   (по статусу) и `ErrorHandler` (транспортная ошибка).

**Границы proxy:** `Next()`, round-robin, `HealthURL`, healthcheck-интеграцию НЕ
трогать. Retry не добавлять.

### `cmd/circuitproxy/main.go`

Изменений НЕ требуется — `NewBalancer(cfg)` уже получает весь `cfg`, breaker
конструируется внутри. Убедиться, что сборка проходит. `TODO(Этап 5)` про
slog/metrics оставить.

---

## Тест-кейсы (реализовать все)

### `internal/breaker/breaker_test.go` (новый файл)

Хелпер `forceOpenedAt` (см. выше). Все прогоны под `-race`.

**Однопоточные:**

1. `TestBreaker_ClosedToOpen_OnThreshold` — `New(Config{FailureThreshold: 3,
   OpenTimeout: time.Second})`; три `Report(false)` подряд; после третьего
   `State()==StateOpen`; `Allow()` возвращает `(false, ErrBreakerOpen)`. Проверить
   также, что после 2 из 3 `Report(false)` state ещё `StateClosed` и `Allow()` даёт
   `true`.
2. `TestBreaker_ClosedSuccessResetsFailures` — порог 3; `Report(false)`,
   `Report(false)`, затем `Report(true)` (сброс), затем ещё два `Report(false)` —
   state всё ещё `StateClosed` (серия прервана, до порога 3 не дошли).
3. `TestBreaker_OpenFastFailsUntilTimeout` — загнать в open (порог 1, один
   `Report(false)`); сразу `Allow()` → `(false, ErrBreakerOpen)`, `State()==StateOpen`
   (таймаут не истёк — `openedAtNanos` только что выставлен, `OpenTimeout` большой).
4. `TestBreaker_OpenToHalfOpen_AfterTimeout` — в open; `forceOpenedAt(time.Now().Add(-time.Hour))`;
   `Allow()` → `(true, nil)` (единственный пробник получил допуск), `State()==StateHalfOpen`.
   Повторный `Allow()` до `Report` → `(false, ErrBreakerOpen)` (пробник уже в полёте).
5. `TestBreaker_HalfOpenToClosed_OnSuccess` — довести до half-open (как в #4);
   `Report(true)` → `State()==StateClosed`, `failures` сброшен (следующий `Allow()`
   → `true`), `trialInFlight` отпущен.
6. `TestBreaker_HalfOpenToOpen_OnFailure` — до half-open; `Report(false)` →
   `State()==StateOpen`, таймаут перезапущен (`Allow()` сразу → fast-fail, т.к.
   `openedAtNanos` обновлён на now). Затем снова `forceOpenedAt(прошлое)` →
   `Allow()` опять пропускает один пробник (доказывает, что half-open→open корректно
   вернул возможность повторной пробы).
7. `TestBreaker_ZeroThresholdOpensOnFirstFailure` — `New(Config{FailureThreshold: 0,
   OpenTimeout: time.Second})`; один `Report(false)` → `State()==StateOpen`
   (порог нормализован до 1). Аналогично проверить `FailureThreshold: -5`.
8. `TestBreaker_ReportFailureWhileOpenIsNoop` — загнать в open (порог 1); запомнить
   `State()==StateOpen`; вызвать `Report(false)` ещё раз напрямую; state остался
   `StateOpen` (защитный no-op не сломал состояние). Опционально: убедиться, что
   `openedAtNanos` не изменился (можно косвенно — через то, что после `forceOpenedAt`
   прошлого один пробник по-прежнему проходит).

**Конкурентный (обязателен, `-race`):**

9. `TestBreaker_HalfOpen_ExactlyOneTrial` — по скелету SKILL.md (строки 79-107):
   `New(Config{FailureThreshold: 1, OpenTimeout: 10*time.Millisecond})`; `Report(false)`
   (в open); `forceOpenedAt(time.Now().Add(-time.Second))`; N=100 горутин ждут общий
   `start`-канал, затем разом `Allow()`; ассерт `granted == 1`. Прогонять устойчиво
   под `-race -count=10` (в отладке `-count=100`).

**Stress (`-race`):**

10. `TestBreaker_StressAllowReport` — M горутин (напр. 50) в цикле K итераций
    (напр. 200) дёргают случайно `Allow()`/`Report(bool)` на одном `Breaker`;
    периодически `forceOpenedAt` для прокрутки переходов. Ассертов на конкретное
    состояние нет — цель в том, чтобы детектор гонок молчал (тест «зелёный без
    data race»). `t.Parallel()` не обязателен; главное `go test -race`.

### `internal/proxy` — интеграционные тесты (`httptest.Server`)

Добавить в `internal/proxy/proxy_test.go` (переиспользовать `mustBalancer`,
`testConfig`). Где нужна инъекция времени в breaker — сконструировать backend с
доступом к его breaker'у. Рекомендуемый подход: тестовый хелпер, который строит
`Balancer` и возвращает доступ к `*Backend`, чтобы вызвать `forceOpenedAt` на его
breaker'е. Т.к. `forceOpenedAt` — метод пакета `breaker` (в `breaker`-тестах), для
proxy-тестов нужен аналог. **Реализовать так:** добавить в `internal/proxy`
（в `proxy.go`, экспортируемо НЕ обязательно — можно в `export_test.go`) тестовый
хук. Предпочтительно — файл `internal/proxy/export_test.go` с хелпером:

```go
// export_test.go — test-only accessors, not part of the package API.
package proxy
import "time"
// forceBreakerOpenedAt rewinds a backend's breaker open-timestamp so the next
// Allow() treats OpenTimeout as elapsed. Test-only.
func (b *Backend) forceBreakerOpenedAt(t time.Time) { b.cb.forceOpenedAt(t) }
```

Но `b.cb.forceOpenedAt` требует, чтобы `forceOpenedAt` был доступен из пакета
`proxy` — он определён в `breaker` и приватен. Поэтому: **вынести инъекцию времени
в breaker в экспортируемый тестовый путь.** Самое чистое для stdlib-only без
усложнения API — дать breaker'у **экспортируемое поле `Config.Now`** (уже есть!) и
в proxy-тестах конструировать backend с управляемым `Now`. Реализация:

- Добавить в `internal/proxy/export_test.go` хелпер, который строит `*Balancer` с
  одним backend'ом, чей breaker использует переданную `now func() time.Time` и
  заданный `breaker.Config`. Напр.:

```go
// export_test.go
package proxy
import (
	"net/url"
	"github.com/akomyagin/circuitproxy/internal/breaker"
)
// newBalancerWithBreaker builds a single-backend balancer whose backend uses the
// given breaker config (allowing injected time via cfg.Now). Test-only.
func newBalancerWithBreaker(rawURL string, cfg breaker.Config) (*Balancer, *Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	be := &Backend{URL: u}
	be.up.Store(true)
	be.cb = breaker.New(cfg)
	bal := &Balancer{backends: []*Backend{be}}
	return bal, be, nil
}
```

Тест управляет временем через `cfg.Now`, указывающий на функцию, читающую
тест-контролируемый `atomic.Int64` (nanos). Сдвиг «вперёд за OpenTimeout» =
запись в этот atomic. Это заменяет `forceOpenedAt` и не требует лезть в приватные
поля breaker'а из другого пакета. **Выбрать этот способ для proxy-тестов.**

Тест-кейсы proxy:

11. `TestHandler_BreakerOpensAndFastFails` — backend-`httptest.Server` со счётчиком
    вызовов (`atomic.Int64`), отдающий 500. `FailureThreshold: N` (напр. 3),
    `OpenTimeout` большой. Слать N+запросов через `Handler()`:
    - Первые N запросов доходят до backend'а (счётчик растёт до N), каждый получает
      500 (ответ backend'а проксируется как есть — проверка: клиент видит 500, НЕ
      503/502), breaker считает неудачи.
    - После N-й неудачи circuit `open`. Следующий запрос: клиент получает **503**
      («circuit breaker open»), а **счётчик backend'а НЕ увеличился** (fast-fail без
      реального обращения) — ключевой ассерт.
12. `TestHandler_BreakerHalfOpenRecovers` — как #11 до open; затем сдвинуть время
    через инъекцию `Now` за `OpenTimeout`; backend переключить на 200; один запрос
    проходит (пробник), получает 200, счётчик backend'а +1; circuit `closed`;
    следующий запрос снова идёт на backend и получает 200. Инъекция времени —
    через `newBalancerWithBreaker` + управляемый `Now`, НЕ real sleep.
13. `TestHandler_BreakerCounts5xxAsFailure` — явный фокус на классификации: backend
    отдаёт 503 (или любой 5xx); после `FailureThreshold` таких ответов circuit
    открывается. (Подтверждает, что `ModifyResponse` считает 5xx неуспехом.)
    Контроль-кейс: backend отдаёт 404 (4xx) многократно — circuit остаётся `closed`
    (4xx = успех для breaker'а, backend продолжает получать запросы).
14. `TestHandler_BreakerCountsTransportErrorAsFailure` — backend `httptest.Server`,
    затем `srv.Close()` (полная недоступность → транспортная ошибка → `ErrorHandler`).
    После `FailureThreshold` запросов к закрытому backend'у circuit открывается:
    следующий запрос fast-fail'ит 503 (а не 502 от `ErrorHandler`) — доказывает, что
    `ErrorHandler` докладывает неудачу и open наступает. Первые N запросов при этом
    отдают клиенту 502 (транспорт упал, реальное обращение было).

Примечание к #11/#13: чтобы клиент увидел код ответа backend'а, использовать
`httptest.NewRecorder()` и читать `rec.Code`. Счётчик обращений к backend'у —
`atomic.Int64` внутри handler'а `httptest.Server`.

---

## Порядок реализации (рекомендуемый)

1. `internal/config`: метод `OpenTimeout()` (тривиально, разблокирует proxy).
2. `internal/breaker`: тела `Allow`/`Report` + нормализация порога в `New` +
   `breaker_test.go` (кейсы 1-10). Прогнать `go test -race -count=10 ./internal/breaker/`.
3. `internal/proxy`: поле `cb`, методы `Allow/Report`, конструирование в
   `NewBalancer`, `Allow()` в `Handler()`, `ModifyResponse`, `Report(false)` в
   `ErrorHandler`, докстринг; `export_test.go` + кейсы 11-14.
4. Финал: `gofmt -l .` (пусто), `go vet ./...`, `go test -race ./...`.

## Границы (НЕ трогать)

- Retry (Этап 4) — не реализовывать. Open = fast-fail, точка. Retry будет через
  `http.RoundTripper` на Этапе 4 (решение принято, handoff Этапа 1 п.2).
- `internal/healthcheck` — не трогать. Liveness (`IsUp/SetUp`) и breaker —
  независимы; breaker health-checker'у не отдавать.
- Схему `config.Config` (JSON-поля/теги) не менять — только метод `OpenTimeout()`.
- Публичные сигнатуры `breaker.New/Allow/Report/State` не менять — только тела.
- Этапы 4/5 заранее не готовить.
