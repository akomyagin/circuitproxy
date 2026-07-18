---
name: go-circuitproxy-dev
description: Конвенции проекта circuitproxy — учебный L7 reverse proxy на Go (stdlib-only) с circuit breaker. Конкурентная модель breaker'а (atomic state machine, гарантия «ровно один» пробный запрос в half-open через CAS-флаг trial-in-flight), паттерн тестирования конкурентности (-race, параллельные горутины во время переходов состояний), паттерн тестовых backend'ов через httptest.Server. Использовать при реализации любого этапа кодирования circuitproxy — особенно кода в internal/breaker (автомат), internal/proxy (round-robin + retry) и internal/healthcheck.
---

# SKILL: go-circuitproxy-dev

Технические конвенции CircuitProxy. Читать при работе над любым кодом проекта,
особенно `internal/breaker`.

## Общее

- Go 1.23+, **только stdlib**. Никаких внешних модулей в MVP.
- `-race` обязателен во всех прогонах тестов: `go test -race ./...`. Проект про
  конкурентность — тест без `-race` не считается пройденным.
- `internal/` — всё непубличное (это приложение, не библиотека).
- Форматирование: `gofmt`. Логирование: `log/slog` (структурное).

## Конкурентная модель circuit breaker (ядро)

Три состояния **на каждый backend**: `closed` / `open` / `half-open`.

### Представление состояния

```go
type Breaker struct {
    state         atomic.Int32 // 0=closed, 1=open, 2=halfOpen
    failures      atomic.Int32 // подряд идущие ошибки в closed
    openedAtNanos atomic.Int64 // время перехода в open (для OpenTimeout)
    trialInFlight atomic.Bool  // «пробник уже в полёте» — сердце half-open

    failureThreshold int32
    openTimeout      time.Duration
    now              func() time.Time // инъекция времени для тестов
}
```

Никаких `sync.Mutex` на горячем пути `Allow()` — только atomic. Мьютекс допустим
только если появится действительно составное состояние, которое нельзя выразить
несколькими atomic-полями (в MVP — не нужен).

### Гарантия «ровно один пробный запрос» в half-open

Два независимых CAS решают две разные гонки:

1. **Гонка «кто переведёт open → half-open».** Когда `OpenTimeout` истёк, много
   горутин одновременно видят «пора». Перевод делается через CAS на `state`:
   ```go
   if b.state.CompareAndSwap(stateOpen, stateHalfOpen) {
       // выиграл перевод — этой горутине можно пробовать (см. п.2)
   }
   // проигравшие видят state == halfOpen и идут в п.2
   ```

2. **Гонка «кто пошлёт единственный пробник».** В half-open допуск даётся через CAS
   на `trialInFlight`:
   ```go
   if b.trialInFlight.CompareAndSwap(false, true) {
       return true, nil        // ЕДИНСТВЕННЫЙ пробник
   }
   return false, ErrBreakerOpen // остальные — fast-fail
   ```

`Report(success)` завершает пробу: `trialInFlight.Store(false)` + перевод состояния
(`success` → `closed` со сбросом `failures`; иначе → `open`, `openedAtNanos` обновить).

**Инвариант, который обязан проверять конкурентный тест:** сколько бы горутин ни
вызвали `Allow()` одновременно в момент open→half-open, ровно ОДНА получит `true`.

### Стратегия не-пробных запросов в half-open

**fast-fail** (не блокирующее ожидание): проигравшие `trialInFlight`-CAS сразу
получают `ErrBreakerOpen`. Не заводить очереди/ожидающие горутины — это post-MVP.

## Паттерн тестирования конкурентности

Конкурентный тест half-open — обязательная часть Этапа 3. Скелет:

```go
func TestBreaker_HalfOpen_ExactlyOneTrial(t *testing.T) {
    b := New(Config{FailureThreshold: 1, OpenTimeout: 10 * time.Millisecond})
    // загнать в open
    b.Report(false)
    // проэмулировать истечение таймаута (инъекция времени, НЕ реальный sleep)
    b.forceOpenedAt(time.Now().Add(-time.Second))

    const N = 100
    var granted atomic.Int32
    var wg sync.WaitGroup
    start := make(chan struct{})
    for i := 0; i < N; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            <-start // максимизировать одновременность старта
            if ok, _ := b.Allow(); ok {
                granted.Add(1)
            }
        }()
    }
    close(start)
    wg.Wait()

    if got := granted.Load(); got != 1 {
        t.Fatalf("half-open пропустил %d пробников, ожидался ровно 1", got)
    }
}
```

Правила:

- Гонять с `-race`. Один прогон недостаточно надёжен для гонок — использовать
  `go test -race -count=100 -run HalfOpen ./internal/breaker` при отладке.
- **Не** полагаться на реальные `time.Sleep` для таймаутов — инъектировать время
  (`now func() time.Time` или тестовый хелпер `forceOpenedAt`). Реальные sleep'ы
  делают тесты флаки и медленными.
- Максимизировать одновременность: все горутины ждут общий `start`-канал, потом
  стартуют разом.
- Stress-тест: N горутин в цикле дёргают `Allow()`/`Report()` — детектор гонок
  должен молчать.

## Паттерн тестовых backend'ов (httptest.Server)

Docker не нужен. Тестовый backend — `httptest.Server` с **переключаемым** handler'ом
для имитации падения/восстановления:

```go
func newFlakyBackend(t *testing.T) (*httptest.Server, *atomic.Bool) {
    healthy := &atomic.Bool{}
    healthy.Store(true)
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !healthy.Load() {
            w.WriteHeader(http.StatusInternalServerError) // «упал»
            return
        }
        w.WriteHeader(http.StatusOK)
    }))
    t.Cleanup(srv.Close)
    return srv, healthy // healthy.Store(false) роняет backend на лету
}
```

- Полная недоступность (не 5xx, а отказ соединения) — `srv.Close()`.
- Health-check тесты — отдельный health-путь с тем же переключателем.
- Retry/backoff тесты — backend-счётчик: первые N запросов → ошибка, дальше → 200;
  проверять число реальных попыток.
- Всё in-process, детерминированно, без сети наружу.

## Идемпотентность в retry (Этап 4)

Ретраятся только идемпотентные методы: GET, HEAD, PUT, DELETE, OPTIONS, TRACE.
POST/PATCH по умолчанию НЕ ретраятся. Тело буферизовать перед первой попыткой, если
идемпотентный запрос с телом может быть повторён. Backoff — экспоненциальный
(`base * 2^n`) + jitter; `sleep` инъектировать для тестируемости.
