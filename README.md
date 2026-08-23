# CircuitProxy

Учебный L7 reverse proxy на Go с встроенными resilience-паттернами: активные
health checks, circuit breaker (closed / open / half-open) и retry с backoff.
Написан с нуля на stdlib — не обёртка над Traefik/nginx.

Фокус проекта — **конкурентно-корректный half-open state**: когда circuit переходит
из `open` в `half-open`, пропускается ровно один пробный запрос, а не лавина под
параллельной нагрузкой. Это главный источник тонких конкурентных багов и главная
причина существования проекта.

## Статус

Этапы 0-5 смержены в `master` — **MVP завершён**: базовый reverse proxy +
round-robin (Этап 1), активный health-check (Этап 2), circuit breaker — ядро
проекта (Этап 3, конкурентно-корректный half-open доказан тестом под `-race`),
retry с экспоненциальным backoff (Этап 4, через кастомный `http.RoundTripper`,
оборачивающий `Transport`), финальная валидация конфига и наблюдаемость (Этап 5:
`slog`-логирование переходов breaker'а, `expvar`-счётчики на отдельном
`metrics_listen`). Дальше — по приоритету
[docs/POST_MVP_PLAN.md](docs/POST_MVP_PLAN.md) (weighted balancing, sticky
sessions, TLS termination, маршрутизация).

История этапов и принятые решения — в `docs/handoff/` (по одному файлу на этап) и
`docs/plans/` (планы реализации).

## Быстрый старт (dev)

```bash
export PATH=$PATH:~/sdk/go/bin
go build ./...
go vet ./...
go test -race ./...

# запуск:
go run ./cmd/circuitproxy -config config.example.json
```

## Документация

- [docs/PLAN.md](docs/PLAN.md) — видение, архитектура, список Этапов, «После MVP».
- [docs/TECHNICAL_PLAN.md](docs/TECHNICAL_PLAN.md) — стек, конкурентная модель
  breaker'а, разбивка по Этапам, решение по Docker.
- [docs/POST_MVP_PLAN.md](docs/POST_MVP_PLAN.md) — weighted balancing, sticky
  sessions, TLS termination, маршрутизация.

## Зависимости

Только Go stdlib. Docker/docker-compose не используется — тестовые backend'ы
поднимаются через `httptest.Server` in-process (см. TECHNICAL_PLAN §«Решение по Docker»).

## Лицензия

MIT — см. [LICENSE](LICENSE).
