# Асинхронный сервис платежей

API принимает платёж и сохраняет его со статусом `pending`. Consumer обрабатывает платёж через RabbitMQ (2–5 с, ~90% успешных) и отправляет результат на `webhook_url`.


## Запуск

```bash
cp .env.example .env
docker compose up --build
```

Миграции применяются автоматически.

- API: http://localhost:8000
- SWAGGER: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672

## API

Все запросы требуют заголовок `X-API-Key` (значение `API_KEY`).

**Создать платёж** (`POST /api/v1/payments`, обязателен `Idempotency-Key`):

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me-in-production' \
  -H 'Idempotency-Key: order-1001' \
  -d '{"amount": "199.90", "currency": "RUB", "description": "Заказ 1001",
       "metadata": {"order_id": 1001}, "webhook_url": "https://example.com/webhook"}'
```

| Ответ | Когда |
|---|---|
| `202` | платёж принят: `payment_id`, `status`, `created_at` |
| `202` | повтор с тем же ключом и телом: возвращается исходный платёж |
| `409` | тот же ключ с другим телом |
| `422` | невалидные данные, нет `Idempotency-Key`, `webhook_url` указывает на localhost или приватный IP |

**Получить платёж:** `GET /api/v1/payments/{payment_id}`.

Статусы: `pending`, `succeeded`, `failed`. Тело вебхука: `{"payment_id", "status", "processed_at"}`.

## Надёжность

```
API ─(одна транзакция)─> payments + outbox ─> outbox publisher ─> payments.new ─> consumer ─> статус + webhook
```

- **Outbox.** Событие не теряется между коммитом и публикацией. После `OUTBOX_MAX_RETRIES` неудачных публикаций запись получает статус `failed`.
- **Без двойной обработки.** Статус меняется условным `UPDATE ... WHERE status='pending'`, поэтому повторная доставка или параллельные consumer'ы не перезапишут результат.
- **Webhook.** До 3 повторов с паузами 1, 2, 4 с. Факт доставки хранится в `payments.webhook_sent_at`: при сбое после смены статуса вебхук отправится при повторной обработке. Гарантия доставки — «хотя бы один раз».
- **Ретраи и DLQ.** При ошибке сообщение публикуется повторно со счётчиком `x-attempt`. После `PAYMENT_MAX_ATTEMPTS` попыток оно уходит в `payments.new.dlq`.
- **SSRF.** Вебхуки на localhost и приватные адреса запрещены, включая домены, которые туда резолвятся.

## Настройки

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `API_KEY` | — (обязательна, ≥16 символов) | ключ для `X-API-Key` |
| `PAYMENT_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `PAYMENT_MAX_ATTEMPTS` | `3` | попыток обработки до DLQ |
| `WEBHOOK_MAX_ATTEMPTS` | `3` | повторов вебхука |
| `WEBHOOK_TIMEOUT_SECONDS` | `10` | таймаут вебхука |
| `OUTBOX_MAX_RETRIES` | `10` | попыток публикации из outbox |
| `ALLOW_PRIVATE_WEBHOOKS` | `false` | разрешить локальные приёмники вебхуков |

## Полезные команды

```bash
uv run pytest   # тесты; PostgreSQL поднимается через testcontainers (нужен Docker)
docker compose logs -f api consumer
docker compose exec api alembic upgrade head
docker compose down -v   # остановить и удалить данные
```
