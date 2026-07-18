# CircuitProxy

Учебный L7 reverse proxy на Go с встроенными resilience-паттернами: активные
health checks, circuit breaker (closed / open / half-open) и retry с backoff.
Написан с нуля на stdlib — не обёртка над Traefik/nginx.

Фокус проекта — **конкурентно-корректный half-open state**: когда circuit переходит
из `open` в `half-open`, пропускается ровно один пробный запрос, а не лавина под
параллельной нагрузкой. Это главный источник тонких конкурентных багов и главная
причина существования проекта.

## Статус

Bootstrap (Этап 0). Реализация ведётся по Этапам — см. документацию.

## Быстрый старт (dev)

```bash
export PATH=$PATH:~/sdk/go/bin
go build ./...
go vet ./...
go test -race ./...

# запуск (после Этапа 1):
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
