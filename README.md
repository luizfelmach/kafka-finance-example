# Projeto Kafka para Detecção de Transações Fraudulentas

## Visão geral

Este documento descreve uma proposta completa de projeto para a disciplina de Sistemas Orientados a Eventos, usando Apache Kafka como infraestrutura principal para monitorar, em tempo real, transações financeiras potencialmente fraudulentas [1].

A proposta segue diretamente o enunciado da atividade, que exige uma arquitetura orientada a eventos com produtores, consumidores e brokers Kafka, além da detecção de situações de interesse a partir de eventos primitivos e da geração de pelo menos um evento derivado ou composto para apoiar ações posteriores [1].

## Objetivo do projeto

O objetivo é simular um sistema financeiro digital capaz de produzir continuamente eventos de transações e eventos de contexto do usuário, processá-los em tempo real e identificar comportamentos suspeitos que indiquem fraude [1].

Quando uma situação de interesse for detectada, o sistema deve reagir por meio de ações como geração de alertas, bloqueio temporário de operações, solicitação de autenticação adicional ou publicação de novos eventos para outros componentes do ecossistema consumirem [1].

## Relação com o enunciado

O projeto atende ao enunciado em quatro pontos centrais:

- Implementa um sistema de monitoramento orientado a eventos com Kafka como middleware de comunicação [1].
- Monitora continuamente eventos produzidos em tempo real por produtores e consumidos por serviços especializados [1].
- Define quatro situações de interesse detectadas a partir de eventos primitivos [1].
- Inclui um caso mais complexo em que um consumidor infere conhecimento e republica eventos derivados no Kafka [1].

Além disso, como o enunciado permite o uso de dados simulados em tempo real quando não houver API pública adequada para o domínio escolhido, a simulação de um ambiente financeiro é plenamente válida para o trabalho [1].

## Domínio escolhido

O domínio proposto é o de um **banco digital com operações financeiras em tempo real**, incluindo principalmente transações PIX, transferências internas, cadastro de favorecidos, tentativas de login, troca de dispositivo, alteração de senha e atualização de limites.

Esse domínio é especialmente adequado porque facilita a criação de eventos diversos, permite demonstrar regras simples e compostas de detecção de fraude e produz uma narrativa de apresentação intuitiva, já que fraude bancária é um problema conhecido e fácil de contextualizar.

## Justificativa da escolha

Fraude em transações financeiras é um caso clássico de monitoramento em tempo real. O sistema precisa reagir rapidamente a comportamentos suspeitos, muitas vezes antes da conclusão da operação.

Com Kafka, é possível desacoplar geração, transporte, processamento e ação sobre os eventos, tornando a arquitetura aderente ao modelo orientado a eventos pedido na atividade [1].

## Arquitetura proposta

A arquitetura pode ser organizada em produtores, tópicos Kafka, consumidores e componentes de visualização.

### Componentes principais

- **Produtor de transações**: gera eventos financeiros continuamente.
- **Produtor de autenticação e contexto**: gera eventos como login, falha de autenticação, troca de dispositivo e alteração de senha.
- **Broker Kafka**: recebe e distribui eventos para os consumidores.
- **Consumidor de regras simples**: detecta situações imediatas a partir de eventos primitivos.
- **Consumidor agregador de risco**: correlaciona múltiplos eventos e produz eventos derivados.
- **Consumidor de decisão/ação**: consome eventos derivados e executa ações como alerta ou bloqueio.
- **Dashboard**: exibe métricas, alertas e estatísticas em tempo real, alinhando-se à funcionalidade desejável de visualização citada no enunciado [1].

### Fluxo de alto nível

1. O simulador produz transações e eventos contextuais.
2. Esses eventos são publicados em tópicos Kafka.
3. Consumidores processam os eventos em tempo real.
4. Regras simples geram alertas imediatos.
5. Um consumidor mais avançado combina múltiplos sinais e produz um evento derivado de risco.
6. Outro consumidor toma a decisão final e publica o resultado.
7. O dashboard mostra os resultados em tempo real.

Esse fluxo está alinhado ao funcionamento geral de um sistema de monitoramento orientado a eventos descrito no enunciado, no qual recursos produzem eventos, consumidores monitoram continuamente esses eventos e ações são executadas quando situações de interesse são detectadas [1].

## Tópicos Kafka sugeridos

A seguir está uma sugestão de tópicos para organizar o projeto:

| Tópico | Finalidade |
|---|---|
| `transactions.raw` | Transações financeiras brutas geradas pelo simulador |
| `auth.events` | Eventos de autenticação, login, falhas e alterações de contexto |
| `customer.profile` | Informações de perfil e comportamento esperado do cliente |
| `fraud.alerts` | Alertas gerados por regras simples ou compostas |
| `risk.scores` | Eventos derivados contendo score de risco |
| `transactions.blocked` | Transações bloqueadas por alto risco |
| `transactions.approved` | Transações aprovadas automaticamente |
| `transactions.review` | Transações enviadas para análise manual |

Essa separação ajuda a demonstrar claramente o papel de cada consumidor e o encadeamento de eventos simples e derivados.

## Modelos de eventos

A seguir estão exemplos de mensagens publicadas em cada tópico.

### `transactions.raw`

Evento primitivo de transação financeira:

```json
{
  "transaction_id": "tx-123",
  "account_id": "acc-88",
  "user_id": "u-7",
  "type": "PIX",
  "amount": 4800.50,
  "timestamp": "2026-04-24T12:00:00Z",
  "origin_country": "BR",
  "device_id": "dev-21",
  "ip_address": "177.10.2.8",
  "merchant_category": "electronics",
  "destination_account": "acc-999",
  "status": "REQUESTED"
}
```

### `auth.events`

Eventos de autenticação e alterações de contexto do usuário:

```json
{
  "event_id": "auth-456",
  "user_id": "u-7",
  "event_type": "password_change",
  "timestamp": "2026-04-24T11:58:00Z",
  "device_id": "dev-21",
  "ip_address": "177.10.2.8"
}
```

Outros `event_type` possíveis: `login`, `failed_auth`, `device_change`, `limit_change`, `beneficiary_added`.

### `customer.profile`

Informações de perfil e comportamento esperado do cliente:

```json
{
  "user_id": "u-7",
  "account_id": "acc-88",
  "average_transaction_amount": 250.00,
  "typical_merchant_categories": ["grocery", "transport"],
  "trusted_devices": ["dev-21", "dev-05"],
  "typical_hours": "08:00-22:00",
  "updated_at": "2026-04-24T10:00:00Z"
}
```

### `fraud.alerts`

Alertas gerados por regras simples ou compostas:

```json
{
  "alert_id": "alert-789",
  "user_id": "u-7",
  "transaction_id": "tx-123",
  "alert_type": "PASSWORD_CHANGE_THEN_HIGH_TRANSACTION",
  "severity": "HIGH",
  "reason": "Senha alterada em 2026-04-24T11:58:00Z, transacao de R$ 4800.50 em 2026-04-24T12:00:00Z (2 min depois)",
  "timestamp": "2026-04-24T12:00:00Z"
}
```

### `risk.scores`

Evento derivado contendo score de risco calculado:

```json
{
  "transaction_id": "tx-123",
  "account_id": "acc-88",
  "user_id": "u-7",
  "risk_score": 85,
  "reasons": [
    "high_amount",
    "new_device",
    "password_change_recent"
  ],
  "decision": "REVIEW",
  "timestamp": "2026-04-24T12:00:05Z"
}
```

### `transactions.blocked`

Transação bloqueada por alto risco:

```json
{
  "transaction_id": "tx-123",
  "account_id": "acc-88",
  "user_id": "u-7",
  "type": "PIX",
  "amount": 4800.50,
  "timestamp": "2026-04-24T12:00:00Z",
  "block_reason": "risk_score_above_threshold",
  "risk_score": 85,
  "blocked_at": "2026-04-24T12:00:05Z"
}
```

### `transactions.approved`

Transação aprovada automaticamente:

```json
{
  "transaction_id": "tx-124",
  "account_id": "acc-88",
  "user_id": "u-7",
  "type": "PIX",
  "amount": 150.00,
  "timestamp": "2026-04-24T12:05:00Z",
  "risk_score": 15,
  "approved_at": "2026-04-24T12:05:01Z"
}
```

### `transactions.review`

Transação enviada para análise manual:

```json
{
  "transaction_id": "tx-123",
  "account_id": "acc-88",
  "user_id": "u-7",
  "type": "PIX",
  "amount": 4800.50,
  "timestamp": "2026-04-24T12:00:00Z",
  "risk_score": 85,
  "review_reason": "multiple_risk_signals",
  "sent_to_review_at": "2026-04-24T12:00:05Z"
}
```

## Situações de interesse mínimas

O enunciado exige pelo menos três situações de interesse detectadas a partir de eventos primitivos consumidos do Kafka [1]. Este projeto define quatro situações mínimas, superando o requisito base.

### Situação 1: transação com valor muito acima do padrão do cliente

Quando uma transação tem valor muito superior ao comportamento histórico da conta, o sistema considera a operação suspeita.

#### Exemplo de regra

- Se `amount > media_ultimas_transacoes * 5`, gerar alerta.

#### Ação

- Publicar evento em `fraud.alerts`.

### Situação 2: muitas transações em uma janela curta de tempo

Quando a mesma conta realiza várias transações em poucos segundos ou minutos, isso pode indicar automação, invasão de conta ou tentativa de esvaziamento de saldo.

#### Exemplo de regra

- Se uma conta gerar mais de 5 transações em 1 minuto, marcar como suspeita.

#### Ação

- Publicar evento em `fraud.alerts`.

### Situação 3: transação em dispositivo não reconhecido

Uma transação originada de um `device_id` nunca antes observado para aquele usuário pode representar tomada de conta ou uso de um aparelho clonado/roubado.

#### Exemplo de regra

- Se `device_id` da transação não existir no histórico confiável do `user_id`, gerar evento suspeito.

#### Ação

- Publicar evento em `fraud.alerts`.

### Situação 4: alteração de senha seguida de transação relevante em poucos minutos

Quando um usuário altera sua senha e, em um curto intervalo de tempo (ex: 5 minutos), realiza uma transação de valor elevado ou fora de seu padrão histórico, isso pode indicar que a conta foi comprometida e o invasor está agindo rapidamente antes que o usuário legítimo perceba a invasão.

#### Exemplo de regra

- Se um evento `password_change_event` ocorrer para um `user_id` e, nos próximos 5 minutos, uma transação desse mesmo `user_id` tiver `amount > 1000` ou `amount > media_historica * 3`, gerar alerta de alto risco.

#### Ação

- Publicar evento em `fraud.alerts` com severidade ALTA.

## Situação complexa com evento derivado

O enunciado pede explicitamente uma situação mais complexa na qual um consumidor processe eventos primitivos, infira conhecimento e produza novamente no Kafka um evento derivado ou composto [1].

A melhor forma de atender a esse requisito é criar um **score de risco** por transação.

### Ideia central

Um consumidor lê eventos de múltiplos tópicos, cruza informações e calcula um valor de risco entre 0 e 100.

Esse consumidor atua em dois papéis:

- **Consumidor**, porque lê eventos primitivos.
- **Produtor**, porque publica um novo evento derivado com o resultado da inferência [1].

### Exemplo de evento derivado

```json
{
  "transaction_id": "tx-123",
  "account_id": "acc-88",
  "risk_score": 91,
  "reasons": [
    "high_amount",
    "new_device",
    "3_transactions_in_2_minutes"
  ],
  "decision": "REVIEW"
}
```

### Exemplo de cálculo de score

Uma lógica simples pode funcionar assim:

- Valor acima do padrão: +35 pontos.
- Novo dispositivo: +25 pontos.
- Alta frequência recente: +20 pontos.
- Horário incomum: +10 pontos.
- Falhas de login recentes: +15 pontos.

A soma pode ser limitada em 100.

### Interpretação do score

| Faixa | Interpretação | Ação |
|---|---|---|
| 0–39 | Baixo risco | Aprovar |
| 40–69 | Médio risco | Gerar alerta e pedir validação extra |
| 70–100 | Alto risco | Bloquear temporariamente ou enviar para revisão |

Esse mecanismo atende de forma muito clara ao requisito de evento derivado e ainda facilita a demonstração em sala.

## Ações possíveis do sistema

Quando uma situação de interesse for detectada, o sistema pode tomar uma ou mais ações, em linha com o enunciado, que menciona notificações, execução de serviços e produção de novos eventos [1].

As ações sugeridas são:

- Publicar alerta em `fraud.alerts`.
- Bloquear a transação e publicar em `transactions.blocked`.
- Aprovar e publicar em `transactions.approved`.
- Encaminhar para análise manual em `transactions.review`.
- Solicitar autenticação adicional.
- Atualizar dashboard em tempo real.
- Registrar log de auditoria.

## Simulação de dados

O enunciado informa que, caso o grupo não queira usar dados reais ou escolha um domínio sem API aberta adequada, é possível simular dados em tempo real para alimentar o sistema [1].

Nesse projeto, a simulação é uma escolha apropriada, porque dados financeiros reais normalmente são sensíveis e difíceis de obter publicamente.

### Estratégia de simulação

O simulador pode criar perfis de clientes com comportamentos distintos:

- **Cliente conservador**: poucas transações e valores baixos.
- **Cliente corporativo**: transações maiores e horário comercial.
- **Cliente frequente**: muitas transações, mas com padrão previsível.
- **Fraudador simulado**: valores altos, novos dispositivos, múltiplos destinatários e horários incomuns.

### Distribuição possível

- 80% de transações normais.
- 20% de transações suspeitas ou fraudulentas.

### Casos simulados de fraude

- Explosão de transações em sequência.
- Transação por dispositivo novo.
- PIX alto após troca de senha.
- Operação em horário atípico.
- Múltiplos destinos inéditos em poucos minutos.

Esse tipo de simulação facilita a demonstração, pois permite controlar exatamente quando as regras devem disparar.

## Tecnologias sugeridas

Uma stack simples e eficaz para o projeto pode ser:

| Camada | Tecnologia sugerida |
|---|---|
| Mensageria | Apache Kafka |
| Execução local | Docker Compose |
| Produtores/consumidores | Python ou Java |
| Processamento de fluxo | Kafka Streams ou lógica própria com consumidores |
| Persistência opcional | PostgreSQL ou MongoDB |
| Visualização | Streamlit, Grafana ou dashboard web simples |

### Sugestão prática para implementação rápida

Se o grupo quiser priorizar simplicidade de entrega, a combinação abaixo tende a ser suficiente:

- Kafka em Docker Compose.
- Produtores e consumidores em Python.
- Simulador gerando JSON.
- Dashboard em Streamlit.

Essa abordagem reduz complexidade operacional e ainda demonstra bem os conceitos centrais do trabalho.

## Organização em microserviços

Uma divisão possível de serviços é a seguinte:

| Serviço | Responsabilidade |
|---|---|
| `transaction-simulator` | Gerar transações financeiras |
| `auth-simulator` | Gerar eventos de autenticação e contexto |
| `rule-engine` | Aplicar regras simples sobre eventos primitivos |
| `risk-aggregator` | Correlacionar eventos e gerar score de risco |
| `decision-engine` | Tomar decisão final sobre a transação |
| `dashboard-service` | Expor visualização em tempo real |

Essa modularização deixa a arquitetura clara e ajuda muito na apresentação do projeto.

## Exemplo de fluxo completo

Um cenário completo de execução pode ser descrito assim:

1. Um usuário faz login a partir de um dispositivo novo.
2. Logo depois, o sistema recebe um evento de alteração de senha.
3. Em seguida, ocorre uma transação PIX de alto valor.
4. O `rule-engine` identifica valor alto e dispositivo desconhecido.
5. O `risk-aggregator` combina os sinais e calcula score 85.
6. O evento derivado é publicado em `risk.scores`.
7. O `decision-engine` consome esse evento e decide bloquear a operação.
8. Um alerta é publicado e o dashboard exibe a ocorrência.

Esse exemplo ajuda a demonstrar, de forma didática, a diferença entre evento primitivo, correlação de eventos e evento derivado.

## Métricas e dashboard

O enunciado lista como funcionalidade desejável a visualização das situações de interesse em dashboards, planilhas ou arquivos [1].

Um dashboard simples pode conter:

- Total de transações recebidas por minuto.
- Quantidade de alertas por tipo.
- Top 10 contas com maior score de risco.
- Distribuição de scores de risco.
- Quantidade de transações aprovadas, bloqueadas e em revisão.
- Linha do tempo de eventos recentes.

### Indicadores visuais úteis

- Verde: transação aprovada.
- Amarelo: alerta gerado.
- Vermelho: transação bloqueada.
- Azul: operação enviada para revisão manual.

## Estrutura sugerida do projeto

Uma estrutura de pastas possível seria:

```text
fraud-detection-kafka/
├── docker-compose.yml
├── producer/
│   ├── transaction_simulator.py
│   └── auth_simulator.py
├── consumers/
│   ├── rule_engine.py
│   ├── risk_aggregator.py
│   └── decision_engine.py
├── dashboard/
│   └── app.py
├── schemas/
│   ├── transaction.json
│   ├── auth_event.json
│   └── risk_score.json
└── README.md
```

Essa estrutura ajuda o grupo a dividir o trabalho entre os integrantes e facilita a manutenção do projeto.

## Possível divisão entre integrantes

Como o projeto pode ser feito em trio, uma divisão equilibrada pode ser [1]:

- **Pessoa 1**: infraestrutura Kafka, Docker Compose e tópicos.
- **Pessoa 2**: simuladores e produtores de eventos.
- **Pessoa 3**: consumidores, score de risco e dashboard.

Se todos quiserem contribuir em apresentação e integração, essa divisão ainda permite um fechamento conjunto.

## Roteiro de demonstração

Para a apresentação de 10 a 15 minutos mencionada no enunciado [1], um roteiro simples pode ser:

1. Apresentar o problema de fraude financeira.
2. Mostrar a arquitetura orientada a eventos.
3. Explicar os tópicos Kafka.
4. Mostrar os eventos primitivos.
5. Explicar as quatro situações de interesse.
6. Demonstrar o evento derivado de score de risco.
7. Executar uma simulação ao vivo.
8. Mostrar o dashboard reagindo em tempo real.

## Diferenciais que podem valorizar o trabalho

Alguns pontos podem deixar o projeto mais interessante:

- Uso de janelas temporais para contar eventos por minuto.
- Explicabilidade do score de risco por meio do campo `reasons`.
- Dashboard com atualização automática.
- Logs de auditoria de decisões.
- Possibilidade de extensão futura para machine learning.

Esses diferenciais não são obrigatórios, mas melhoram a qualidade técnica e a narrativa da apresentação.

## Limitações e simplificações aceitáveis

Como se trata de um projeto acadêmico, algumas simplificações são aceitáveis:

- O score pode ser baseado em regras, sem necessidade de modelo de IA.
- O histórico do usuário pode ficar em memória ou em banco simples.
- O simulador pode gerar perfis artificiais em vez de integrar uma API real.
- A autenticação adicional pode ser apenas simulada por um novo evento.

Essas simplificações mantêm o projeto viável sem comprometer os requisitos do enunciado.

## Conclusão

A proposta de um sistema de detecção de transações fraudulentas com Kafka é totalmente compatível com a atividade, pois permite construir uma arquitetura orientada a eventos com produtores, consumidores e brokers Kafka, definir situações de interesse a partir de eventos primitivos e produzir eventos derivados para decisões mais complexas [1].

Além de ser tecnicamente adequada, essa proposta também é forte para apresentação, porque combina um problema realista, uma arquitetura clara, regras fáceis de explicar e uma demonstração visual convincente com alertas e dashboard em tempo real [1].
