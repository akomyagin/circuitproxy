# План Этапа 2 — Активный health-check

Ветка: `stage-2-healthcheck` (от `master`, HEAD `8fd50e9`).
Исполнитель: обычный `model: opus` кодер по этому файлу. План конкретный — пути,
сигнатуры, тест-кейсы даны явно.

## Цель

`internal/healthcheck` активно опрашивает каждый backend на health-пути с заданным
интервалом и таймаутом, помечает backend up/down, а `Balancer.Next()` учитывает эти
флаги: пропускает down-backend'ы и возвращает `nil`, если все down. Health-checker
останавливается чисто по `context.Context`. Интеграция в `cmd/circuitproxy/main.go`:
запуск горутины под уже существующим сигнальным `ctx`, чистая остановка при shutdown.

## Границы (НЕ трогать)

- `internal/breaker` — Этап 3, не реализовывать и не импортировать.
- Retry / RoundTripper — Этап 4. Не проектировать. Решение Этапа 2 не должно
  противоречить будущему ретраю через кастомный `http.RoundTripper` (см. handoff п.2):
  ничего в схеме `Balancer`/`Backend`, что мешало бы позже обернуть `Transport`.
- Схема `config.Config` (JSON-поля верхнего уровня) — не менять. Разрешено добавить
  **только** метод-хелпер `Timeout()` рядом с `Interval()` (поле `TimeoutSeconds` уже
  есть).
- Этапы 3/5 заранее не готовить.

---

## Архитектурное решение (ПРИНЯТО — реализовать именно так)

### Проблема

`Backend.up` — неэкспортируемое `atomic.Bool` в пакете `proxy`. `Balancer.backends`
неэкспортируемо. `healthcheck.Checker` должен на каждом тике переключать liveness
конкретных backend'ов, а `Balancer.Next()` — эти флаги учитывать. Сейчас у healthcheck
нет ни доступа к списку backend'ов, ни экспортируемого мутатора.

### Нестыковка в TECHNICAL_PLAN.md — фиксируем корректно

TECHNICAL_PLAN.md (строка ~44) объявляет слой `healthcheck → зависит от breaker`. Это
**ошибка**: Этапу 2 `breaker` не нужен вообще. Health-check логически обращается к
типу `proxy.Backend` (переключить его liveness) и к health-URL (производному от
`Backend.URL`). Корректная зависимость:

```
config     → ни от кого
breaker    → ни от кого
proxy      → от breaker (Этап 3) + config
healthcheck → от proxy + config      ← ИСПРАВЛЕНО (было "от breaker")
cmd        → собирает всё
```

`healthcheck` зависит от `proxy`, **не наоборот** — `proxy` про `healthcheck` ничего не
знает. Циклической зависимости `proxy ↔ healthcheck` нет. Это направление проверено по
факту использования: healthcheck читает `Backend.URL` и пишет liveness; proxy лишь
предоставляет тип и мутатор, потребителя не импортирует.

> Действие для исполнителя: в TECHNICAL_PLAN.md строку про слои `healthcheck` привести
> в соответствие (это делает шаг 7 пайплайна — финальная актуализация доков; в коде
> просто реализовать корректную зависимость). В самом коде — направление
> `healthcheck → proxy`.

### Выбор: вариант (a) — экспортируемый мутатор на `Backend` + аксессор на `Balancer`

Из двух кандидатов handoff-файла выбран **(a)**, отклонён **(b)**.

**Что делаем (a):**
- Инкапсулировать `atomic.Bool` за методами `Backend.SetUp(bool)` / `Backend.IsUp() bool`
  (поле `up` остаётся неэкспортируемым — наружу торчат только методы).
- Добавить `Balancer.Backends() []*Backend` — health-checker получает список для обхода.
- Добавить `Backend.HealthURL(path string) string` — единая точка построения health-URL
  из `Backend.URL` + `cfg.Path`, чтобы логика склейки жила в `proxy` рядом с `URL`, а не
  размазывалась по healthcheck.

**Почему (a), а не (b):**
1. **(b) ломает сигнатуру `NewBalancer` и владение пулом.** Вариант (b) требует, чтобы
   пул `[]*Backend` строился снаружи (в healthcheck или cmd) и передавался в
   `NewBalancer`. Но пул с его валидацией URL уже живёт в `NewBalancer` (proxy.go:36-53)
   и обязан там жить: balancer без валидного пула — ошибка конструирования. Инвертировать
   владение — значит разнести валидацию и хранение по двум пакетам ради одного мутатора.
2. **Минимальный диф, ясное владение.** (a) добавляет три метода и меняет одну строку в
   `Next()`; владение пулом остаётся в `Balancer` (единственный владелец), healthcheck —
   лишь читатель-мутатор через аксессор. Это ровно та инкапсуляция, которую cross-file и
   reuse углы ревью Этапа 1 просили (handoff п.1).
3. **Не мешает Этапу 4.** Ретрай (RoundTripper) будет звать `Balancer.Next()` за
   следующим живым backend'ом — та же публичная поверхность, что и сейчас. `Backends()`
   и `SetUp/IsUp` ортогональны ретраю, конфликта нет.
4. **`LivenessSetter`-интерфейс (подвариант b) — избыточен для MVP.** Один конкретный
   реализатор (`*proxy.Backend`), один потребитель (`healthcheck`). Интерфейс здесь —
   абстракция без второй реализации; stdlib-first проект её не оправдывает. Если Этап 3/5
   заведёт второго потребителя liveness — интерфейс вводится тогда, дёшево (методы уже
   есть). Сейчас healthcheck принимает `[]*proxy.Backend` напрямую.

---

## Изменения по файлам

### 1. `internal/config/config.go`

Добавить метод `Timeout()` симметрично `Interval()` (после строки 70):

```go
// Timeout returns the per-probe HTTP timeout as a time.Duration.
func (h HealthCheckConfig) Timeout() time.Duration {
	return time.Duration(h.TimeoutSeconds) * time.Second
}
```

Больше в этом файле ничего. JSON-схема не меняется.

### 2. `internal/proxy/proxy.go`

**2.1. Инкапсулировать liveness (поле `up` остаётся неэкспортируемым).**
Добавить методы к `Backend` (после определения структуры, ~строка 24):

```go
// IsUp reports whether the backend is currently marked live by the health
// checker. Safe for concurrent use.
func (b *Backend) IsUp() bool { return b.up.Load() }

// SetUp marks the backend live (true) or down (false). Called by the health
// checker; safe for concurrent use.
func (b *Backend) SetUp(up bool) { b.up.Store(up) }

// HealthURL builds the absolute health-probe URL for this backend by joining
// its base URL with path. path is the health endpoint from config (e.g. "/health").
func (b *Backend) HealthURL(path string) string {
	u := *b.URL // copy; do not mutate the shared base URL
	u.Path = path
	return u.String()
}
```

Замечание по `HealthURL`: копируем `*b.URL` по значению и присваиваем `Path` напрямую
(перезапись, не join) — health-путь в конфиге абсолютный (`/health`). `RawQuery`/фрагмент
не переносим (health-проба без query). Это детерминированно и достаточно для MVP.

**2.2. Аксессор к пулу.** Добавить к `Balancer`:

```go
// Backends returns the balancer's backend pool for the health checker to probe.
// The slice is the balancer's own backing store; callers must not append to or
// reorder it, only read entries and toggle their liveness via SetUp.
func (b *Balancer) Backends() []*Backend { return b.backends }
```

**2.3. `Next()` учитывает liveness (закрывает TODO(Этап 2) на строках 60-61).**
Заменить тело `Next()` так, чтобы оно пропускало down-backend'ы и возвращало `nil`,
когда все down. Требование: остаться на atomic hot-path без мьютексов; сохранить
round-robin характер (равномерность по живым backend'ам).

```go
// Next returns the next live backend round-robin, or nil if all backends are
// down. Selection stays lock-free: a single atomic increment picks a starting
// slot, then we scan forward at most n slots for a live backend.
func (b *Balancer) Next() *Backend {
	n := uint64(len(b.backends))
	if n == 0 {
		return nil
	}
	start := b.counter.Add(1) - 1
	for i := uint64(0); i < n; i++ {
		idx := (start + i) % n
		be := b.backends[idx]
		if be.IsUp() {
			return be
		}
	}
	return nil
}
```

Обоснование алгоритма (записать кратко и в комментарий кода):
- Один `counter.Add(1)` за вызов сохраняет round-robin-распределение стартовой точки —
  как в Этапе 1 (handoff явно фиксирует формулу `(Add(1)-1) % n` как корректную, её
  сохраняем для стартового индекса).
- Линейный скан вперёд до `n` слотов пропускает down-backend'ы; первый живой —
  результат. Худший случай O(n), n мало (единицы backend'ов) — приемлемо, мьютексов не
  нужно.
- Все down → цикл проходит все n слотов, ни один не `IsUp()` → `nil`. Обработчик уже
  возвращает 503 при `nil` (proxy.go:114-116) — не менять.
- Гонка «liveness меняется во время скана» безопасна: `IsUp()` атомарен, худшее —
  выбрали backend, ставший down на следующем такте, либо пропустили только что
  поднявшийся; на следующем запросе поправится. Это допустимо (eventually consistent
  ротация), явных инвариантов не нарушает.

Обновить/убрать `TODO(Этап 2)` в докстринге `Next()`.

### 3. `internal/healthcheck/healthcheck.go`

Переписать заглушку. Итоговый вид:

```go
// Package healthcheck actively probes backends on an interval and marks them
// up/down so the balancer can exclude unavailable backends from rotation (Этап 2).
package healthcheck

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/akomyagin/circuitproxy/internal/config"
	"github.com/akomyagin/circuitproxy/internal/proxy"
)

// Checker periodically probes backends and updates their liveness flags.
type Checker struct {
	cfg      config.HealthCheckConfig
	backends []*proxy.Backend
	client   *http.Client
}

// New constructs a health Checker for the given backends. The per-probe timeout
// comes from cfg.Timeout().
func New(cfg config.HealthCheckConfig, backends []*proxy.Backend) *Checker {
	return &Checker{
		cfg:      cfg,
		backends: backends,
		client:   &http.Client{Timeout: cfg.Timeout()},
	}
}

// Run probes all backends once immediately, then on every cfg.Interval() tick,
// until ctx is cancelled. It blocks; callers run it in a goroutine.
func (c *Checker) Run(ctx context.Context) {
	c.probeAll(ctx)

	ticker := time.NewTicker(c.cfg.Interval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probeAll(ctx)
		}
	}
}

// probeAll probes every backend sequentially and updates its liveness.
func (c *Checker) probeAll(ctx context.Context) {
	for _, be := range c.backends {
		c.probe(ctx, be)
	}
}

// probe sends one GET to the backend's health URL and toggles its liveness.
// Any transport error or non-2xx status marks the backend down; a 2xx marks it up.
func (c *Checker) probe(ctx context.Context, be *proxy.Backend) {
	healthURL := be.HealthURL(c.cfg.Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		c.mark(be, false, healthURL, "build request", err)
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.mark(be, false, healthURL, "request failed", err)
		return
	}
	defer resp.Body.Close()

	up := resp.StatusCode >= 200 && resp.StatusCode < 300
	if up {
		c.mark(be, true, healthURL, "", nil)
	} else {
		c.mark(be, false, healthURL, "non-2xx status", nil)
	}
}

// mark applies liveness and logs a transition only when it changes, to avoid
// per-tick log spam.
func (c *Checker) mark(be *proxy.Backend, up bool, healthURL, reason string, err error) {
	prev := be.IsUp()
	be.SetUp(up)
	if prev == up {
		return
	}
	if up {
		slog.Info("backend recovered", "backend", healthURL)
	} else {
		slog.Warn("backend marked down", "backend", healthURL, "reason", reason, "err", err)
	}
}
```

Требования к реализации healthcheck (исполнителю соблюсти точно):
- **Немедленная первая проба** до первого тика: без неё все backend'ы до первого
  интервала числятся up по стартовому значению из `NewBalancer` (proxy.go:50) — если
  backend реально мёртв, первый интервал он ошибочно в ротации. `probeAll(ctx)` до
  цикла закрывает это.
- **Один `http.Client` с `Timeout: cfg.Timeout()`** на весь Checker, не на пробу.
  `NewRequestWithContext(ctx, ...)` — чтобы отмена `ctx` (shutdown) прерывала
  in-flight пробу, а не ждала её таймаута.
- **`resp.Body.Close()` обязателен** (иначе утечка соединений). `defer` сразу после
  проверки `err`.
- **Критерий up = 2xx.** Любой не-2xx или транспортная ошибка → down. Просто и
  однозначно для MVP.
- **Лог только на смену состояния** (`mark` сравнивает с `IsUp()` до записи) —
  избегаем спама «backend up» каждый тик. `slog` — как везде в проекте.
- **Последовательный обход backend'ов** в `probeAll` — для MVP достаточно (backend'ов
  единицы, таймаут ограничивает суммарное время). Параллелить не нужно; проще и без
  веера горутин. Если позже понадобится — отдельная заметка, не сейчас.
- Поле `up` в `Backend` мутируется **только** через `SetUp` — прямого доступа нет
  (неэкспортируемо), инкапсуляция соблюдена.

### 4. `cmd/circuitproxy/main.go`

Завести health-checker под существующим сигнальным `ctx` (строка 54) — тем же, что
отменяется на SIGINT/SIGTERM. Запустить `Run` в горутине **до** `srv.ListenAndServe`.

Изменения:
1. Добавить импорт `"github.com/akomyagin/circuitproxy/internal/healthcheck"`.
2. Заменить блок `TODO(Этап 2)` (строки 57) на инстанцирование и запуск. Разместить
   **после** создания `ctx` (строка 54-55), **до** запуска сервера (строка 60-63):

```go
	checker := healthcheck.New(cfg.HealthCheck, balancer.Backends())
	go checker.Run(ctx)
```

Обоснование места и остановки:
- `checker.Run(ctx)` блокируется, поэтому в горутине.
- Использует **тот же `ctx`**, что и сигнальный: при SIGINT/SIGTERM `ctx` отменяется →
  `Run` выходит из select по `ctx.Done()` → горутина завершается. Отдельный
  shutdown-путь для checker'а не нужен — он read/write-only по atomic-флагам, его
  «дренаж» тривиален (текущая in-flight проба прервётся отменой ctx через
  `NewRequestWithContext`).
- Явного `wg.Wait()` на завершение checker-горутины не добавляем: она не держит
  ресурсов, требующих упорядоченного закрытия (клиент без явного пула-владельца,
  `ticker.Stop()` в defer внутри `Run`). Усложнять graceful shutdown ради этого не
  нужно — DoD этого не требует. (Если ревью настоит — это отдельная заметка, не блокер.)
- `TODO(Этап 5)` про slog/metrics на строке 58 оставить как есть.

---

## Тест-кейсы

Файлы: `internal/healthcheck/healthcheck_test.go` (основные), плюс дополнить/создать
`internal/proxy/proxy_test.go` тестами `Next()` с liveness. Все под `go test -race`.
Паттерн backend'а — `httptest.Server` с переключаемым handler'ом через `atomic.Bool`
(SKILL.md, строки 122-147). Инъекция времени: для healthcheck используем **короткие
реальные интервалы** (`time.Ticker` — часть контракта Run), но не полагаемся на сон
для синхронизации: ждём наблюдаемого эффекта через polling-хелпер с дедлайном (см.
ниже), а не фиксированный `time.Sleep`.

### A. `internal/proxy` — `Balancer.Next()` с liveness

Строить `Balancer` через `NewBalancer` с фейковыми URL (валидными абсолютными строками,
серверы поднимать не обязательно — `Next()` только читает флаги).

1. **`TestNext_AllUp_RoundRobin`** — 3 backend'а, все up. N вызовов `Next()`
   распределяются round-robin (каждый получает ~N/3, порядок циклический). Проверяет,
   что рефактор не сломал равномерность Этапа 1.
2. **`TestNext_SkipsDown`** — 3 backend'а, средний `SetUp(false)`. Многократный `Next()`
   никогда не возвращает down-backend; возвращает только два живых, по кругу.
3. **`TestNext_AllDown_ReturnsNil`** — все `SetUp(false)` → `Next()` возвращает `nil`
   (несколько вызовов подряд, все `nil`).
4. **`TestNext_RecoveryReturnsToRotation`** — один up из трёх; после `SetUp(true)` на
   ранее down-backend он снова появляется в выдаче `Next()`.
5. **`TestNext_Concurrent`** (`-race`) — M горутин параллельно дёргают `Next()`, часть
   горутин параллельно `SetUp` тоглит флаги; ассерт: детектор гонок молчит, и любой
   ненулевой результат — всегда `IsUp()==true` на момент проверки не гарантируется
   (eventually consistent), поэтому ассертить только отсутствие гонки и что `nil`
   возвращается лишь когда на момент вызова живых не было. Практически: гоняем
   `-count` и проверяем, что паники/гонки нет; функциональный ассерт — «пока есть хотя
   бы один стабильно up, Next() не отдаёт nil» на подмножестве вызовов со всеми up.

### B. `internal/healthcheck` — Checker

Хелпер ожидания эффекта (вместо фиксированного sleep):

```go
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() { return }
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
```

Backend-хелпер: `httptest.Server`, health-handler на `cfg.Path`, отвечает 200 при
`healthy.Load()==true`, иначе 500. Конфиг: `Path: "/health"`, малый интервал
(`IntervalSeconds` → через хелпер, либо построить `HealthCheckConfig{}` напрямую с
`interval_seconds`≈мс-уровня; т.к. `Interval()` умножает на секунду, для тестов
удобнее сконструировать Checker с уже коротким интервалом — **см. примечание ниже**).

> Примечание по интервалу в тестах: `Interval()` = `IntervalSeconds * time.Second`,
> минимум 1s — для тестов это медленно. Варианта два, выбрать первый:
> (1) в тестах строить `Checker` литералом (пакет тот же — `healthcheck`, доступ к
> неэкспортируемым полям есть в `_test.go` того же пакета), задавая `cfg` с нужным
> `Path`/`Timeout`, а короткий тик получать, вызывая `probeAll(ctx)` напрямую в
> проверках перехода вместо ожидания `ticker.C`. То есть переходы up/down тестировать
> через прямой вызов `probeAll`, а отдельным тестом проверить, что `Run` действительно
> тикает и останавливается по ctx.
> (2) если хочется гонять полный `Run` — вынести интервал так, чтобы тест мог задать
> суб-секундный (НЕ менять JSON-схему; допустимо в тесте сконструировать `Checker` и
> подменить его тикер-интервал через неэкспортируемое поле, если добавить его). Не
> усложнять — предпочесть (1).

Тест-кейсы:

1. **`TestChecker_ProbeMarksDown`** — backend отвечает 500; после `probeAll(ctx)`
   соответствующий `*proxy.Backend` имеет `IsUp()==false`.
2. **`TestChecker_ProbeMarksUp`** — backend 200; стартово `SetUp(false)`; после
   `probeAll` → `IsUp()==true`.
3. **`TestChecker_TransportErrorMarksDown`** — backend закрыт (`srv.Close()` до пробы,
   имитация отказа соединения per SKILL.md 143); после `probeAll` → `IsUp()==false`.
4. **`TestChecker_DownThenRecover`** — backend 200 → `probeAll` (up) → `healthy.Store(false)`
   → `probeAll` (down) → `healthy.Store(true)` → `probeAll` (up снова). Проверяет полный
   цикл падение/восстановление на одном backend.
5. **`TestChecker_RunImmediateProbe`** — backend стартово мёртв (500); запустить
   `go checker.Run(ctx)`; `waitFor` до `IsUp()==false` в пределах < одного интервала —
   доказывает, что `Run` пробит **немедленно**, не ждёт первого тика.
6. **`TestChecker_RunStopsOnCtxCancel`** — запустить `Run` в горутине с отменяемым
   `ctx`; отменить; ассертить, что горутина завершилась (напр. закрыть `done`-канал
   после `Run` вернулся, `waitFor`/`select` с таймаутом). Доказывает чистую остановку.
7. **`TestChecker_RunTicksRepeatedly`** (опционально, если интервал субсекундный
   доступен) — backend up→down между тиками; убедиться, что `Run` подхватывает смену на
   следующем тике без ручного `probeAll`. Если интервал только секундный — покрыть
   логику тиков кейсом 6 + прямыми `probeAll` из кейсов 1-4 и этот кейс пропустить,
   отметив причину.
8. **`TestChecker_ConcurrentProbe`** (`-race`) — несколько backend'ов, `Run` в горутине
   + параллельное чтение `IsUp()` из других горутин; детектор гонок молчит.

Интеграционный (по желанию, усиливает DoD, не обязателен):

9. **`TestIntegration_DownBackendExcludedFromRouting`** — поднять 2 `httptest.Server`
   backend'а + `Balancer` + `Checker`; уронить один (health 500); `waitFor` пока
   `Next()` перестанет его отдавать; проверить, что прокси-трафик идёт только на живой;
   поднять обратно — возвращается. Живёт в `internal/healthcheck` или отдельном
   `internal/proxy` integration-тесте (пакет решает исполнитель; healthcheck уже
   импортирует proxy, обратного импорта нет — держать в healthcheck).

---

## Критерий готовности (DoD)

- `go build ./...` чист.
- `go vet ./...` чист.
- `gofmt -l .` пусто.
- `go test -race ./...` зелёный, включая новые тесты A и B.
- `Balancer.Next()` пропускает down-backend'ы и возвращает `nil`, когда все down;
  TODO(Этап 2) в `Next()` снят.
- `healthcheck.Checker` реально опрашивает backend'ы, помечает up/down, логирует только
  переходы, останавливается по `ctx`.
- `cmd/circuitproxy/main.go` запускает checker в горутине под сигнальным `ctx`;
  TODO(Этап 2) на строке 57 снят.
- `config.Timeout()` добавлен.
- Инкапсуляция соблюдена: поле `Backend.up` осталось неэкспортируемым, мутация только
  через `SetUp`.
- Направление зависимости `healthcheck → proxy` (не наоборот), цикла нет:
  `go build ./...` это подтверждает (цикл импорта не скомпилировался бы).

## Заметки для шага 7 (актуализация доков — НЕ в этом кодинге)

- TECHNICAL_PLAN.md строку слоёв `healthcheck → от breaker` заменить на
  `healthcheck → от proxy + config`.
- Handoff-пункт 1 (Backend.up доступ) — закрыт вариантом (a); отметить резолюцию.
- MEMORY: `stage1_open_questions` — вопрос Backend.up-доступа решён.
