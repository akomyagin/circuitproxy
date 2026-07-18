# CLAUDE.md

Guidance for Claude Code when working in the **CircuitProxy** repository.

## Что это

Учебный L7 reverse proxy на Go (stdlib-only) с circuit breaker'ом. Соло-проект,
AI-ассистированная разработка. Не бизнес-идея — цель техническая: resilience-паттерны
и конкурентная корректность. Бюджет: $0.

Ядро сложности — конкурентно-корректный **half-open** circuit breaker (ровно один
пробный запрос под параллельной нагрузкой). См. [docs/TECHNICAL_PLAN.md](docs/TECHNICAL_PLAN.md).

## Конвенции

- **Язык:** документация и subject коммитов — по-русски; код, идентификаторы и
  комментарии в коде — по-английски.
- **Коммиты:** conventional-commit с русским subject, напр.
  `feat(breaker): реализовать конкурентно-корректный half-open`. Заканчивать
  трейлером `Co-Authored-By: Claude`.
- **Зависимости:** только stdlib в MVP. Любая внешняя зависимость — только с явным
  обоснованием (см. POST_MVP_PLAN).
- **Docker:** не используется — тестовые backend'ы через `httptest.Server`.

## Команды

```bash
export PATH=$PATH:~/sdk/go/bin
go build ./...
go vet ./...
go test -race ./...          # -race ОБЯЗАТЕЛЕН — проект про конкурентность
go test ./internal/breaker/  # один пакет
gofmt -l .                   # форматирование (должно быть пусто)
```

Перед коммитом: `gofmt -l .` пусто, `go vet ./...` и `go test -race ./...` зелёные.

## Пайплайн разработки Этапа (проверенный портфельный, НЕ Fable-based)

1. **Opus 4.8** — планирование Этапа + написание кода.
2. **Sonnet 5** (основной чат) — проверка тестового покрытия, написание/дополнение
   тестов, проверка работоспособности (`go test -race ./...`).
3. **Opus** через **Agent-тул** (`model: opus`) — независимое ревью `/code-review`
   на diff ветки.
4. Цикл исправлений — до **3 итераций** (Sonnet правит по замечаниям ревью).
5. **Commit + push + PR** (conventional-commit, русский subject) в `master`.

## Git-workflow

- Новая ветка от `master` на **каждый Этап**: `git checkout -b stage-N-<topic>`.
- Работа → PR → merge в `master`. Не коммитить напрямую в `master`.
- Не коммитить и не пушить, пока пользователь явно не попросит.

## Структура

```
cmd/circuitproxy/   # CLI-точка входа
internal/config/    # парсинг/валидация JSON-конфига
internal/proxy/     # ReverseProxy + round-robin + retry
internal/breaker/   # circuit breaker state machine (atomic) — ЯДРО
internal/healthcheck/ # активный health-check
docs/               # PLAN, TECHNICAL_PLAN, POST_MVP_PLAN
```

Конвенции проекта (конкурентная модель breaker'а, паттерны тестирования гонок) —
в [.claude/skills/go-circuitproxy-dev/SKILL.md](.claude/skills/go-circuitproxy-dev/SKILL.md).
