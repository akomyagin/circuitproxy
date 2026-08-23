# Handoff — 2026-08-23: Этап 3 реализован, готов к коммиту/PR

## Статус

- Ветка `stage-3-circuit-breaker` (от `master`, HEAD `b494576`), изменения в рабочем
  дереве, **ещё не закоммичены** — коммит делается по явной команде пользователя.
- `go build`/`go vet`/`gofmt -l`/`go test -race ./...` — зелёные;
  `go test -race -count=10 ./internal/breaker/...` — зелёный (DoD плана).
- План этапа: [docs/plans/stage-3-circuit-breaker.md](../plans/stage-3-circuit-breaker.md).

## Как прошёл Этап 3

Полный пайплайн: Opus-план (с явным решением архитектуры интеграции breaker↔proxy,
проверенным сверкой с исходником stdlib) → Opus-кодинг → проверка тестов основной
сессией (включая обязательный `-count=10` на конкурентном пакете) → 8 параллельных
Opus-агентов ревью → разбор находок основной сессией (проверка по file:line) → фикс
подтверждённого бага → финальный `-race` прогон.

## Архитектурные решения

1. **Место breaker'а** — неэкспортируемое поле `cb *breaker.Breaker` в `proxy.Backend`
   (симметрично `up` из Этапа 2), доступ через `Backend.Allow()/Report(bool)`.
   Health-checker breaker не получает — liveness (`IsUp/SetUp`) и circuit breaker
   независимы, как и было задокументировано в TECHNICAL_PLAN.
2. **Хуки в `httputil.ReverseProxy`** — `Allow()` во внешнем handler'е сразу после
   `Next()` (fast-fail `503` до реального обращения к backend'у); `Report()` через
   два хука: существующий `ErrorHandler` (транспортная ошибка → неуспех) и новый
   `ModifyResponse` (классификация по статус-коду). **Критично:** `ModifyResponse`
   всегда возвращает `nil` — если бы он вернул ошибку, stdlib вызвал бы
   `ErrorHandler` повторно (двойной `Report` + подмена 5xx-ответа backend'а нашим
   502). Это сверено построчно с `$GOROOT/src/net/http/httputil/reverseproxy.go` на
   этапе планирования И независимо перепроверено углом `cross-file` на ревью.
3. **Критерий успех/неуспех** — транспортная ошибка и `StatusCode >= 500` = неуспех;
   `< 500` (включая 4xx) = успех (4xx — вина клиента, не показатель нездоровья
   backend'а).
4. **HTTP-статус при открытом circuit** — `503` (единый класс с «нет backend'ов»;
   `502` оставлен строго за реальной транспортной ошибкой).

## Найденное и исправленное в ревью

Один реальный баг, независимо подтверждён 2 углами ревью (removed-behavior,
line-by-line), лично перепроверен по file:line:

- **[internal/breaker/breaker.go](../../internal/breaker/breaker.go) —
  `OpenTimeout <= 0` (config опускает `open_timeout_seconds`) схлопывал `open` в
  нулевую длительность.** `Allow()` в состоянии `open` сравнивает `elapsed <
  OpenTimeout`; при `OpenTimeout == 0` это `elapsed < 0`, что ложно для любого
  неотрицательного `elapsed`, — то есть открытый circuit пропускал пробный запрос
  **немедленно** вместо fast-fail, теряя всю защиту cool-down окна. Та же природа
  проблемы, что `interval_seconds=0`/`timeout_seconds=0` на Этапе 2. Фикс:
  `defaultOpenTimeout = 30s`, применяется в `New()` когда `cfg.OpenTimeout <= 0`
  (по аналогии с уже принятой нормализацией `FailureThreshold <= 0 → 1`).
  Регрессионный тест `TestBreaker_ZeroOpenTimeoutUsesDefault`.

## Осознанно НЕ исправленное — держать в голове

Ревью (углы reuse, simplification) нашло мелкие дубликаты тестового кода в
`internal/proxy/proxy_test.go` (повторяющийся boilerplate `httptest.Server`+
счётчик в 4 тестах; повторный паттерн "toggleable health handler" — уже есть как
`newFlakyBackend` в `internal/healthcheck/healthcheck_test.go`, но неэкспортируем
из другого пакета; лишняя инъекция `clock` в 3 из 4 breaker-интеграционных тестов,
где время не сдвигается). **Не исправлено осознанно** — тот же прецедент, что
установлен на Этапе 2 (handoff-2026-08-23-stage2, п.2): вынос в `internal/testutil`
ради нескольких мелких хелперов на масштабе MVP избыточен. Пересмотреть, если
тестовых пакетов с одинаковыми фикстурами станет три и больше.

## Что НЕ нужно перепроверять

- Выбор `admitHalfOpenProbe()` (приватный метод) вместо `fallthrough` для перехода
  open→half-open — план допускал оба варианта, кодер выбрал метод как более чистый;
  ревью (simplification) подтвердило, что вариант чист, не пересматривать.
- Двойной `Report`/подмена ответа при ненулевом `ModifyResponse` error — проверено
  ДВАЖДЫ (планирование + ревью cross-file угол) сверкой с исходником stdlib
  `reverseproxy.go`. `ModifyResponse` в реализации всегда возвращает `nil` — не
  менять без переоткрытия этого вопроса.
- Порядок записи в `Report` half-open (`trialInFlight.Store(false)` последним,
  после установки целевого `state`) — обоснован и не пересматривать.
