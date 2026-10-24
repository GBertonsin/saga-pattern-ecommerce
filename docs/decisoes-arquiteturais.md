# Decisões Arquiteturais — Saga "Create Order"

Este documento registra as decisões técnicas da solução no formato **ADR (Architecture Decision Record)**, os **riscos mitigados**, o tratamento de **dead-letter queues** e as **estratégias de resiliência** (Timeout, Retry, Circuit Breaker, Bulkhead).

---

## Parte 1 — Registro de Decisões (ADRs)

### ADR-01 — Usar o padrão Saga em vez de transação distribuída (2PC)

- **Status:** Aceita
- **Contexto:** Order, Inventory e Payment possuem bancos independentes. O fluxo de compra precisa ser atômico do ponto de vista do negócio.
- **Decisão:** Implementar o fluxo como uma **Saga** de transações locais com compensações.
- **Alternativas rejeitadas:**
  - *2PC / XA:* bloqueia recursos, depende de coordenador síncrono, não é suportado pelo PSP externo nem pelo Kafka, reduz disponibilidade (teorema CAP: escolhemos disponibilidade + tolerância a partição).
  - *Banco compartilhado:* recria o monólito de dados e acopla os times.
- **Consequências:** (+) alta disponibilidade e escalabilidade; (−) consistência apenas eventual, necessidade de compensações e de contramedidas para falta de isolamento.

### ADR-02 — Estilo de Saga: Orquestração

- **Status:** Aceita
- **Contexto:** Fluxo com ordem rígida, uma etapa externa pivô (pagamento) e necessidade de detectar timeouts.
- **Decisão:** **Saga Orquestrada**, orquestrador `CreateOrderSaga` dentro do Order Service, estado persistido em `saga_instance`.
- **Justificativa (atributos de qualidade):**

| Atributo | Impacto da decisão |
|---|---|
| Acoplamento | Participantes não conhecem uns aos outros; não há dependência cíclica. |
| Manutenibilidade | Fluxo inteiro em uma máquina de estados versionada e testável isoladamente. |
| Confiabilidade | Timeouts detectáveis (há "alguém esperando a resposta"). |
| Observabilidade | Estado de cada pedido consultável em uma tabela. |
| Escalabilidade | Orquestrador sem estado em memória; escala por partição do `sagaId`. |
| Disponibilidade | Várias réplicas do orquestrador; Kafka replicado (RF=3). |

- **Consequências:** (+) clareza e controle; (−) risco de concentrar lógica de negócio no orquestrador — mitigado mantendo nele **apenas a sequência**, enquanto as regras (ex.: "tem estoque?") ficam nos participantes.

### ADR-03 — Comunicação assíncrona via Apache Kafka (comando/resposta)

- **Status:** Aceita
- **Decisão:** Tópicos `inventory.commands`, `payment.commands` (comandos), `order-saga.replies` (respostas) e `order.events` (eventos de integração). Chave da mensagem = `orderId`, garantindo **ordem por pedido** dentro da partição.
- **Síncrono apenas onde necessário:** Cliente → API Gateway → Order Service (responde `202 Accepted`) e Payment Service → PSP (API externa HTTP).
- **Consequências:** desacoplamento temporal (um participante fora do ar não derruba a compra — as mensagens aguardam); entrega **at-least-once**, o que exige consumidores idempotentes (ADR-05).

### ADR-04 — Transactional Outbox + CDC para publicar mensagens

- **Status:** Aceita
- **Problema:** *dual write* — gravar no banco e publicar no Kafka são duas operações; se o serviço cair entre elas, o estado fica inconsistente.
- **Decisão:** Cada serviço grava a mensagem de saída na tabela `outbox` **na mesma transação local** da mudança de negócio. O **Debezium** lê o log de transações (CDC) e publica no Kafka.
- **Consequências:** garantia de que "mudou o estado ⇔ mensagem será publicada"; possibilidade de publicação duplicada (resolvida pela ADR-05).

### ADR-05 — Idempotent Consumer em todos os participantes

- **Status:** Aceita
- **Decisão:** Toda mensagem carrega um `messageId` (UUID). Cada consumidor executa, em uma mesma transação: (1) `INSERT` em `processed_messages(consumer, message_id)` com PK única; (2) a mudança de negócio; (3) a mensagem de saída na outbox. Se o `INSERT` violar a PK, a mensagem já foi processada → apenas confirma (*ack*).
- **Complemento:** chaves de negócio únicas (`reservations.order_id`), transições condicionais (`UPDATE ... WHERE status = 'RESERVED'`) e `Idempotency-Key` nas chamadas ao PSP.
- **Retenção:** `processed_messages` é limpa após 7 dias (bem acima da janela máxima de reentrega).

### ADR-06 — Classificação das transações (compensável, pivô, retriável)

- **Status:** Aceita
- **Decisão:** T1 (criar pedido) e T2 (reservar estoque) são **compensáveis**; T3 (autorizar pagamento) é o **pivô**; T4 (confirmar reserva) e T5 (aprovar pedido) são **retriáveis**.
- **Justificativa:** colocar o pagamento — a etapa com maior chance de falha de negócio e com efeito externo — como pivô **minimiza o que precisa ser compensado** e evita estornos desnecessários. Etapas posteriores não podem falhar por regra de negócio (o estoque já está reservado), então só precisam de retry.

### ADR-07 — Contramedidas para a falta de isolamento (ACD sem "I")

Sagas não têm isolamento: outras transações enxergam estados intermediários. Contramedidas adotadas:

| Anomalia | Exemplo | Contramedida |
|---|---|---|
| *Dirty read* | Cliente vê pedido "aprovado" que depois é cancelado | **Semantic lock:** pedido fica `PENDING` até o fim da Saga; UI mostra "processando". |
| *Lost update* | Cancelamento manual do cliente durante a Saga | Pedido `PENDING` rejeita alterações externas; cancelamento do cliente vira um **comando à Saga**. |
| Leitura de estoque intermediário | Outro pedido vê estoque reservado como disponível | Separação `available` × `reserved` no Inventory (**valor reservado explícito**). |
| Compensação antes da ação | `ReleaseStock` chega antes de `ReserveStock` | **Tombstone** `CANCELLED` + operações **comutativas**. |

---

## Parte 2 — Riscos mitigados

| # | Risco | Probabilidade / Impacto | Mitigação |
|---|---|---|---|
| R1 | Cobrança do cliente sem pedido aprovado | Baixa / **Crítico** | Pagamento é o pivô; idempotência no PSP; `RefundPayment` em timeout indefinido; reconciliação diária com extrato do PSP. |
| R2 | Estoque preso em reservas órfãs | Média / Alto | `ReleaseStock` na compensação + `expires_at` na reserva (liberação automática pelo Inventory, independente do orquestrador). |
| R3 | Cobrança duplicada por retry | Média / **Crítico** | `Idempotency-Key = {sagaId}-PAYMENT` no PSP + `processed_messages`. |
| R4 | Mensagem perdida entre banco e broker (*dual write*) | Média / Alto | Transactional Outbox + CDC. |
| R5 | Mensagem processada duas vezes | **Alta** / Médio | Idempotent Consumer em todos os serviços. |
| R6 | Saga "parada" sem resposta | Média / Alto | `deadline_at` + Timeout Scheduler + retries + compensação. |
| R7 | PSP lento ou fora do ar derrubando o Payment Service | Média / Alto | Timeout curto, Circuit Breaker, Bulkhead; comandos aguardam no Kafka. |
| R8 | *Poison message* travando uma partição | Baixa / Alto | Retry limitado → Dead-Letter Topic; a partição continua fluindo. |
| R9 | Compensação que falha | Baixa / Alto | Compensações idempotentes e sem regra de negócio; retry infinito com backoff; após limite → `FAILED_NEEDS_ATTENTION` + alerta. |
| R10 | Orquestrador como ponto único de falha | Baixa / Alto | Réplicas múltiplas, estado durável no banco, `FOR UPDATE SKIP LOCKED`, *consumer group* no Kafka. |
| R11 | Mudança de contrato quebrando consumidores | Média / Médio | Schema Registry (Avro/JSON Schema) com compatibilidade *backward*; campo `schemaVersion`. |
| R12 | Respostas fora de ordem / antigas | Média / Médio | Validação de `current_step` + optimistic locking (`version`) no orquestrador. |

---

## Parte 3 — Dead-Letter Queues (DLT)

### 3.1 Quando uma mensagem vai para a DLT

Distinguimos dois tipos de erro no consumidor:

| Tipo de erro | Exemplos | Tratamento |
|---|---|---|
| **Transitório** (técnico) | Timeout de banco, deadlock, broker indisponível, PSP 503 | **Retry** com backoff exponencial (1s, 2s, 4s, 8s, 16s + jitter) em tópicos de retry (`*.retry-1`, `*.retry-2`) para não bloquear a partição. |
| **Permanente** (não recuperável por retry) | JSON inválido, schema incompatível, campo obrigatório ausente, bug | Vai **direto para a DLT**, sem retry. |
| **Transitório que não se resolve** | Retries esgotados (5 tentativas) | Vai para a **DLT**. |

> Recusa de pagamento ou falta de estoque **não são erros técnicos**: são **respostas de negócio** (`PaymentDeclined`, `StockUnavailable`) e seguem o fluxo normal da Saga. Nunca vão para a DLT.

### 3.2 Topologia

```
inventory.commands ──► consumer ──(erro transitório)──► inventory.commands.retry-1 ──► ... ──► inventory.commands.DLT
payment.commands   ──► consumer ──(erro permanente)─────────────────────────────────────────► payment.commands.DLT
order-saga.replies ──► orquestrador ──(erro)──► order-saga.replies.retry-1 ──► ... ──► order-saga.replies.DLT
```

### 3.3 Conteúdo da mensagem na DLT

A mensagem original é preservada **sem alterações**, acrescida de headers de diagnóstico: `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-exception-class`, `x-exception-message`, `x-retry-count`, `x-failed-at`, `x-saga-id`.

### 3.4 Operação da DLT

1. **Alerta** automático (Prometheus/Alertmanager) quando a DLT recebe qualquer mensagem — DLT de checkout deve ficar vazia.
2. A Saga correspondente é marcada `FAILED_NEEDS_ATTENTION` e aparece no **painel de operações** com o pedido e o motivo.
3. Após a correção (deploy de bugfix, ajuste de dado), um **reprocessador** republica a mensagem no tópico original — seguro porque todos os consumidores são idempotentes.
4. Se não for possível reprocessar, há **ação manual registrada em auditoria** (ex.: estorno manual pelo time financeiro).
5. Retenção da DLT: 14 dias.

---

## Parte 4 — Estratégias de resiliência

### 4.1 Timeout

| Ponto | Timeout | Motivo |
|---|---|---|
| Payment → PSP (HTTP) | conexão 1s, leitura 5s | Evita threads presas em gateway lento. |
| Passo `RESERVING_STOCK` (saga) | 30s | Operação local rápida; atraso indica falha. |
| Passo `AUTHORIZING_PAYMENT` (saga) | 45s | Inclui retries internos do Payment ao PSP. |
| Reserva de estoque (`expires_at`) | 15 min | Rede de segurança caso o orquestrador falhe por completo. |

### 4.2 Retry

- **Somente para erros transitórios** e **somente para operações idempotentes**.
- **Backoff exponencial com jitter** (evita *thundering herd* quando um serviço volta): `espera = min(base × 2^n, máx) + aleatório(0..base)`.
- **Retry budget:** no máximo 3 tentativas internas ao chamar o PSP; acima disso, o erro sobe para o mecanismo de retry do Kafka/Saga (evita amplificação de retries em cascata).
- Erros 4xx do PSP (ex.: `402 declined`, `400 invalid card`) **não** são repetidos — são resultado de negócio.

### 4.3 Circuit Breaker (Payment → PSP)

```
       falhas ≥ 50% em janela de 20 chamadas
CLOSED ───────────────────────────────────────► OPEN
  ▲                                              │ espera 30s
  │ 3 chamadas de teste com sucesso              ▼
  └─────────────────────────────────────── HALF-OPEN
                 (falha → volta para OPEN)
```

- Implementação: Resilience4j (ou equivalente).
- **Estado OPEN:** o Payment Service **não chama o PSP** e **não confirma (ack) o comando**: pausa o consumo do tópico `payment.commands`. Os comandos ficam aguardando no Kafka e a Saga, no estado `AUTHORIZING_PAYMENT`, tem seu `deadline_at` estendido enquanto o circuito estiver aberto (evita compensações em massa por indisponibilidade momentânea do gateway).
- Chamadas lentas (> 3s) também contam como falha (*slow call rate*).
- **Fallback opcional:** PSP secundário (multi-adquirência), com a mesma chave de idempotência por provedor.

### 4.4 Bulkhead

- Pool de conexões HTTP dedicado ao PSP (ex.: 50 conexões), separado do pool que atende consultas de status — lentidão no PSP não derruba o resto do Payment Service.
- Consumidores Kafka de comandos e de consultas em *consumer groups* separados.

### 4.5 Rate limiting e backpressure

- API Gateway limita requisições por cliente; picos são absorvidos pelo Kafka (fila), e os serviços consomem no ritmo que suportam.

### 4.6 Observabilidade

- **Correlation ID** (`sagaId`) propagado em todos os headers e logs; **tracing distribuído** com OpenTelemetry.
- Métricas: sagas por estado, duração média por passo, taxa de compensação, mensagens em DLT, estado do circuit breaker, *consumer lag*.
- Alertas: taxa de compensação acima do normal, qualquer saga em `FAILED_NEEDS_ATTENTION`, circuit breaker aberto > 5 min.

### 4.7 Reconciliação

Job diário compara `payments` (AUTHORIZED/REFUNDED) com o extrato do PSP e `orders` com `reservations`. Divergências geram tickets para o time financeiro/operações — última linha de defesa da consistência eventual.

---

## Parte 5 — Resumo dos cenários de falha cobertos

| Cenário | Comportamento esperado |
|---|---|
| Sem estoque | `StockUnavailable` → `CancelOrder` → `COMPENSATED` |
| Pagamento recusado | `PaymentDeclined` → `ReleaseStock` → `CancelOrder` → `COMPENSATED` |
| Timeout no estoque | Reenvio com mesmo `messageId` → se esgotar, `ReleaseStock` (no-op ou tombstone) → `CancelOrder` |
| Timeout no pagamento | Reenvio + consulta no PSP → se indefinido, `RefundPayment` → `ReleaseStock` → `CancelOrder` |
| PSP fora do ar | Circuit breaker abre, comandos aguardam no Kafka, prazo estendido |
| Mensagem duplicada | Descartada por `processed_messages` / chaves de negócio |
| Compensação chega antes da ação | Tombstone impede a ação atrasada |
| Falha após o pivô | Retry até sucesso; se esgotar → DLT + `FAILED_NEEDS_ATTENTION` |
| Queda do Order Service no meio da Saga | Estado persistido; ao voltar, o scheduler retoma pelas `deadline_at` vencidas |
| Mensagem malformada | DLT imediata + alerta |
