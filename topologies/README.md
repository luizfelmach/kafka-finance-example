# Topologias Kafka Streams — Detecção de Fraude

Fluxogramas das 9 topologias de detecção de fraude em formato Mermaid.js.

## Como usar

- **Mermaid Live Editor**: https://mermaid.live
- **GitHub**: funciona nativamente com blocos ````mermaid`
- **draw.io**: "Arrange > Insert > Advanced > Mermaid"
- **Obsidian**: suporte nativo

## Legenda

| Símbolo | Significado |
|---------|-------------|
| `[tópico]` | Tópico Kafka (source ou sink) |
| `[operação]` | Operação DSL do Kafka Streams |
| `{decisão}` | Branch condicional (filter, join) |
| `[(store)]` | State store (RocksDB) |
| `---` | Fluxo de dados |
| `-.-` | Lookup/consulta a state store |

## Topologias

| # | Arquivo | Caso | Tipo de Janela |
|---|---------|------|----------------|
| 01 | `01-high-amount.mermaid` | Transação de alto valor | Stateless (filter) |
| 02 | `02-burst.mermaid` | Explosão de transações | Tumbling 5min |
| 03 | `03-unknown-device.mermaid` | Dispositivo desconhecido | GlobalKTable join |
| 04 | `04-password-change.mermaid` | Alteração de senha | Stateless (filter) |
| 05 | `05-account-takeover.mermaid` | Tomada de conta | Stateless (filter) |
| 06 | `06-emptying-account.mermaid` | Esvaziamento de conta | Sliding 10min |
| 07 | `07-parallel-login.mermaid` | Login paralelo | Session 10s gap |
| 08 | `08-faraway-login.mermaid` | Login distante | Processor API + KV |
| 09 | `09-under-observation.mermaid` | Sob observação | Aggregate stateful |
| 10 | `10-global-overview.mermaid` | Visão geral do sistema | — |
