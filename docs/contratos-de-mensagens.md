# Contratos de Comunicação — Saga "Create Order"

Todas as mensagens trafegam em JSON (validadas por Schema Registry) e compartilham o mesmo **envelope**.

## 1. Envelope padrão

```json
{
  "messageId": "6f1c2a7e-3b0d-4e7f-9a51-0c2d1e8b4f33",
  "messageType": "ReserveStock",
  "schemaVersion": 1,
  "sagaId": "a8e4b1c0-77d2-4c0a-9f1e-2b3c4d5e6f70",
  "correlationId": "a8e4b1c0-77d2-4c0a-9f1e-2b3c4d5e6f70",
  "causationId": "id-da-mensagem-que-originou-esta",
  "replyTo": "order-saga.replies",
  "occurredAt": "2026-10-24T17:45:12.345Z",
  "payload": { }
}
```

| Campo | Uso |
|---|---|
| `messageId` | Chave de deduplicação (Idempotent Consumer). Reenvios por timeout **reutilizam** o mesmo valor. |
| `sagaId` / `correlationId` | Liga a resposta à instância da saga. |
| `causationId` | Rastreabilidade (qual mensagem causou esta). |
| `replyTo` | Tópico de resposta (participantes não conhecem o orquestrador). |
| `schemaVersion` | Evolução compatível de contrato. |

**Chave de partição Kafka:** `orderId` → todas as mensagens de um pedido ficam na mesma partição e em ordem.

## 2. API síncrona de entrada

`POST /orders` — Header obrigatório `Idempotency-Key: <uuid>`

```json
// Request
{ "customerId": "c-123", "items": [{ "sku": "SKU-1", "quantity": 2, "unitPrice": 150.00 }],
  "paymentToken": "tok_visa_xxx", "currency": "BRL" }

// Response 202 Accepted
{ "orderId": "o-789", "status": "PENDING", "statusUrl": "/orders/o-789" }
```

`GET /orders/{id}` → `PENDING | APPROVED | CANCELLED` (+ `cancellationReason`).

## 3. Comandos (Orquestrador → Participantes)

| Comando | Tópico | Payload | Respostas possíveis | Tipo |
|---|---|---|---|---|
| `ReserveStock` | `inventory.commands` | `orderId`, `items[{sku, quantity}]` | `StockReserved {reservationId, expiresAt}` / `StockUnavailable {skus[]}` | Transação T2 |
| `ConfirmReservation` | `inventory.commands` | `orderId`, `reservationId` | `ReservationConfirmed` | Transação T4 |
| `ReleaseStock` | `inventory.commands` | `orderId`, `reservationId?` | `StockReleased` | **Compensação C2** |
| `AuthorizePayment` | `payment.commands` | `orderId`, `customerId`, `amount`, `currency`, `paymentToken` | `PaymentAuthorized {paymentId, pspTransactionId}` / `PaymentDeclined {reason}` | Transação T3 (pivô) |
| `RefundPayment` | `payment.commands` | `orderId`, `paymentId?`, `amount` | `PaymentRefunded` | **Compensação C3** |

Todas as respostas vão para `order-saga.replies`.

## 4. Eventos de integração (publicados ao fim da Saga)

| Evento | Tópico | Payload | Consumidores |
|---|---|---|---|
| `OrderApproved` | `order.events` | `orderId`, `customerId`, `total`, `items` | Notificação, Logística, Analytics |
| `OrderCancelled` | `order.events` | `orderId`, `reason` (`OUT_OF_STOCK` \| `PAYMENT_DECLINED` \| `TIMEOUT`) | Notificação, Analytics |

## 5. Exemplos

```json
{
  "messageId": "b2...", "messageType": "PaymentDeclined", "schemaVersion": 1,
  "sagaId": "a8e4...", "causationId": "id-do-AuthorizePayment",
  "occurredAt": "2026-10-24T17:45:14.002Z",
  "payload": { "orderId": "o-789", "reason": "INSUFFICIENT_FUNDS", "pspCode": "51" }
}
```

```json
{
  "messageId": "c3...", "messageType": "ReleaseStock", "schemaVersion": 1,
  "sagaId": "a8e4...", "replyTo": "order-saga.replies",
  "occurredAt": "2026-10-24T17:45:14.120Z",
  "payload": { "orderId": "o-789", "reservationId": "r-456" }
}
```

## 6. Regras de compatibilidade

- Apenas mudanças **backward compatible** sem incrementar versão (adicionar campo opcional).
- Remoção/renomeação de campo → nova `schemaVersion` e período de convivência das duas versões.
- Consumidores **ignoram campos desconhecidos** (tolerant reader).
