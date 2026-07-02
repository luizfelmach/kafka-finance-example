# Kafka Finance Example — Sistema de Detecção de Fraudes

Sistema de detecção de fraudes financeiras em tempo real utilizando **Apache Kafka** como plataforma de streaming de eventos e **Kafka Streams** como motor de processamento CEP (Complex Event Processing). O projeto simula transações bancárias, eventos de autenticação e detecta padrões suspeitos através de topologias Kafka Streams com Álgebra de Allen, janelamento temporal e agregações com estado.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Tecnologias](#tecnologias)
- [Pré-requisitos](#pré-requisitos)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configuração e Execução](#configuração-e-execução)
- [Comandos Make Disponíveis](#comandos-make-disponíveis)
- [Tipos de Fraude Detectados](#tipos-de-fraude-detectados)
- [Tópicos Kafka](#tópicos-kafka)
- [Modelos de Dados](#modelos-de-dados)
- [Produtores de Eventos](#produtores-de-eventos)
- [Etapa 2 — Migração para Kafka Streams e CEP (Plano de Implementação)](#etapa-2-—-migração-para-kafka-streams-e-cep-plano-de-implementação)
  - [Fase 1: Atualizar Modelos de Dados](#fase-1-atualizar-modelos-de-dados)
  - [Fase 2: Infraestrutura — pom.xml](#fase-2-infraestrutura---pomxml)
  - [Fase 3: Serdes para Kafka Streams](#fase-3-serdes-para-kafka-streams)
  - [Fase 4: Topologias Kafka Streams (9 topologias)](#fase-4-topologias-kafka-streams-9-topologias)
  - [Fase 5: Back-end Spring Boot](#fase-5-back-end-spring-boot)
  - [Fase 6: Produtores de Teste](#fase-6-produtores-de-teste)
  - [Fase 7: Makefile — Targets Atualizados](#fase-7-makefile---targets-atualizados)
  - [Dependências entre Fases](#dependências-entre-fases)
  - [Conceitos Utilizados](#conceitos-utilizados)
- [Licença](#licença)

---

## Visão Geral

Este projeto demonstra um pipeline completo de detecção de fraudes financeiras:

1. **Geração de clientes simulados** — perfis com contas, dispositivos confiáveis, IPs e coordenadas geográficas.
2. **Produção de eventos legítimos** — transações e autenticações normais.
3. **Simulação de fraudes** — produtores especializados injetam comportamentos maliciosos.
4. **Detecção em tempo real com Kafka Streams** — 9 topologias analisam padrões, joins, janelas temporais e relações de Allen, gerando alertas.

---

## Arquitetura

```
                        PRODUTORES
 ┌─────────────────┐  ┌──────────────────────────────────┐
 │ LegitimateEvent │  │        Fraud Producers            │
 │   Producer      │  │  (7 tipos + 3 de teste)          │
 └────────┬────────┘  └────────┬────────┬────────┬───────┘
          │                    │        │        │
          ▼                    ▼        ▼        ▼
 ┌──────────────┐   ┌──────────────┐   ┌────────────────┐   ┌──────────────────┐
 │ transactions │   │  auth.events │   │  fraud.events  │   │ clients.profiles │
 │    .raw      │   │              │   │                │   │   (compactado)   │
 │ (3 partições)│   │ (3 partições)│   │ (3 partições)  │   │   (1 partição)   │
 │   RF = 3     │   │   RF = 3     │   │   RF = 3       │   │    RF = 3        │
 └──────┬───────┘   └──────┬───────┘   └───────┬────────┘   └────────┬─────────┘
        │                  │                   │                     │
        └──────────────────┼───────────────────┼─────────────────────┘
                           ▼                   ▼
              ┌────────────────────────────────────────────┐
              │           KAFKA STREYS (9 topologias)       │
              │                                            │
              │  ┌──────────┐ ┌──────────┐ ┌───────────┐  │
              │  │ High     │ │  Burst   │ │  Unknown  │  │
              │  │ Amount   │ │          │ │  Device   │  │
              │  └──────────┘ └──────────┘ └───────────┘  │
              │  ┌──────────┐ ┌──────────┐ ┌───────────┐  │
              │  │ Password │ │ Account  │ │ Emptying  │  │
              │  │ Change   │ │ Takeover │ │ Account   │  │
              │  └──────────┘ └──────────┘ └───────────┘  │
              │  ┌──────────┐ ┌──────────┐ ┌───────────┐  │
              │  │ Parallel │ │ Faraway  │ │   Under   │  │
              │  │  Login   │ │  Login   │ │Obser vation│  │
              │  └──────────┘ └──────────┘ └───────────┘  │
              │                                            │
              │  State Stores (RocksDB) | Interactive Q.   │
              └────────────────────┬───────────────────────┘
                                   │
                                   ▼ (alertas)
                            ┌──────────────┐
                            │ fraud.events │
                            └──────┬───────┘
                                   │
                                   ▼
                         ┌─────────────────────┐
                         │  BACK-END SPRING BOOT │
                         │  SSE + REST IQ       │
                         └─────────────────────┘
```

---

## Tecnologias

| Tecnologia | Versão | Descrição |
|------------|--------|-----------|
| Java | 17 | Linguagem principal |
| Apache Kafka | 3.7.1 / 3.8.0 | Plataforma de streaming |
| Kafka Streams | 3.8.0 | API de processamento de streams (CEP) |
| Spring Boot | 3.2.5 | Framework web back-end |
| Maven | — | Build e dependências |
| Docker / Docker Compose | — | Cluster Kafka local |
| Jackson | 2.17.0 | Serialização JSON |
| SLF4J | 2.0.13 | Logging |

---

## Pré-requisitos

- **Java 17** ou superior
- **Maven 3.8+**
- **Docker** e **Docker Compose**
- **Spring Boot 3.2.5** (gerenciado via Maven, sem instalação manual)

---

## Estrutura do Projeto

```
.
├── compose.yaml                    # Docker Compose — cluster Kafka (3 brokers)
├── Makefile                        # Comandos utilitários
├── pom.xml                         # Configuração Maven
├── clients.json                    # Perfis de clientes gerados
├── kafka/
│   ├── broker1.properties          # Config broker 1
│   ├── broker2.properties          # Config broker 2
│   └── broker3.properties          # Config broker 3
└── src/main/java/com/frauddetection/
    ├── config/
    │   └── KafkaConfig.java        # Configuração de brokers, tópicos e Streams
    ├── model/
    │   ├── TransactionEvent.java   # Evento de transação
    │   ├── AuthEvent.java          # Evento de autenticação
    │   └── FraudAlert.java         # Alerta de fraude
    ├── serialization/
    │   ├── JsonSerializer.java
    │   ├── JsonSerde.java          # Serde genérico para Kafka Streams
    │   ├── TransactionEventDeserializer.java
    │   ├── AuthEventDeserializer.java
    │   └── FraudAlertDeserializer.java
    ├── producers/
    │   ├── LegitimateEventProducer.java
    │   ├── HighAmountFraudProducer.java
    │   ├── BurstTransactionFraudProducer.java
    │   ├── UnknownDeviceFraudProducer.java
    │   ├── PasswordChangeFraudProducer.java
    │   ├── AccountTakeoverFraudProducer.java
    │   ├── EmptyingAccountFraudProducer.java
    │   ├── ParallelLoginFraudProducer.java
    │   ├── FarawayLoginFraudProducer.java
    │   └── UnderObservationFraudProducer.java
    ├── streams/                               # 9 topologias Kafka Streams
    │   ├── FraudDetectionTopology.java        # Main que registra todas
    │   ├── HighAmountTopology.java            # aggregate() + Sliding Window
    │   ├── BurstTopology.java                 # groupByKey + Tumbling Window
    │   ├── UnknownDeviceTopology.java         # GlobalKTable Join
    │   ├── PasswordChangeTopology.java        # KStream-KStream Join
    │   ├── AccountTakeoverTopology.java       # Duplo KStream-KStream Join
    │   ├── EmptyingAccountTopology.java       # aggregate() + Tumbling Window
    │   ├── ParallelLoginTopology.java         # Session Window + Allen
    │   ├── FarawayLoginTopology.java          # Processor API + KVStore
    │   └── UnderObservationTopology.java      # Tumbling Window + count
    ├── web/
    │   ├── FraudDetectionApplication.java     # Main Spring Boot
    │   ├── AlertSSEController.java            # SSE endpoint
    │   └── InteractiveQueryController.java    # REST para State Stores
    └── utils/
        ├── ClientGenerator.java
        ├── ClientLoader.java
        └── ClientProfile.java
```

---

## Configuração e Execução

### 1. Iniciar o cluster Kafka

```bash
make up
```

Sobe 3 brokers Kafka em containers Docker com configuração KRaft (sem ZooKeeper).

- **Broker 1** → `localhost:19092`
- **Broker 2** → `localhost:29092`
- **Broker 3** → `localhost:39092`

### 2. Criar os tópicos

```bash
make topics
```

Cria os 4 tópicos:

| Tópico | Partições | RF | Tipo |
|--------|-----------|----|------|
| `transactions.raw` | 3 | 3 | Eventos de transação |
| `auth.events` | 3 | 3 | Eventos de autenticação |
| `fraud.events` | 3 | 3 | Alertas de fraude |
| `clients.profiles` | 1 | 3 | Tabela compactada de perfis (GlobalKTable) |

### 3. Gerar clientes simulados e popular tópico

```bash
make clients
```

Gera `clients.json` com 100 perfis (contas, dispositivos, coordenadas) **e** publica cada perfil no tópico `clients.profiles` (chave=`userId`, cleanup policy = compact).

### 4. Compilar o projeto

```bash
make build
```

Gera o JAR sombreado em `target/kafka-finance-example-1.0-SNAPSHOT.jar`.

### 5. Executar a topologia Kafka Streams

```bash
make run-streams-topology
```

Inicia as 9 topologias de detecção em uma JVM dedicada. As topologias consomem dos tópicos de entrada e produzem alertas para `fraud.events`.

### 6. Executar back-end web (opcional)

```bash
make run-web-backend
```

Inicia o servidor Spring Boot com SSE em tempo real e Interactive Queries.

---

## Comandos Make Disponíveis

### Infraestrutura

| Comando | Descrição |
|---------|-----------|
| `make up` | Inicia o cluster Kafka via Docker Compose |
| `make down` | Para e remove os containers |
| `make restart` | Reinicia os serviços recriando os containers |

### Build

| Comando | Descrição |
|---------|-----------|
| `make build` | Compila o projeto com Maven |
| `make clean` | Remove artefatos de build |

### Dados

| Comando | Descrição |
|---------|-----------|
| `make clients` | Gera `clients.json` + publica perfis no tópico `clients.profiles` |
| `make simulate` | Executa o produtor de eventos legítimos |

### Produtores de Fraude

| Comando | Tipo de Fraude |
|---------|----------------|
| `make high-amount` | Transação com valor anormalmente alto |
| `make burst` | Rajada de transações em curto período |
| `make unknown-device` | Transação a partir de dispositivo desconhecido |
| `make password-change` | Alteração de senha suspeita |
| `make account-takeover` | Sequência: login suspeito → troca de senha → transação de alto valor |
| `make emptying-account` | Tentativa de esvaziar a conta |

### Kafka Streams

| Comando | Descrição |
|---------|-----------|
| `make run-streams-topology` | Executa as 9 topologias Kafka Streams |
| `make run-web-backend` | Executa o back-end Spring Boot (porta 8080) |

### Testes das Novas Topologias

| Comando | Descrição |
|---------|-----------|
| `make test-parallel-login` | Testa detecção de logins paralelos (Allen: overlaps) |
| `make test-faraway-login` | Testa detecção de login geograficamente impossível (Allen: before) |
| `make test-under-observation` | Testa detecção de conta sob observação |

### Kafka Utils

| Comando | Descrição |
|---------|-----------|
| `make topics` | Cria os tópicos necessários |
| `make topics-view` | Lista todos os tópicos |
| `make topics-describe` | Exibe detalhes dos tópicos da aplicação |
| `make listen TOPIC=<nome>` | Escuta um tópico no console |

---

## Tipos de Fraude Detectados

| Tipo | Descrição | Lógica de Detecção |
|------|-----------|--------------------|
| **HIGH_AMOUNT** | Transação com valor muito acima da média histórica | `aggregate()` + Sliding Window (5 min): se primeira tx > R$ 8.000 OU valor > 3× média |
| **BURST_TRANSACTIONS** | Muitas transações em curto intervalo | `groupByKey()` + `count()` + Tumbling Window (60s): ≥ 5 transações |
| **UNKNOWN_DEVICE** | Dispositivo não cadastrado realiza transação | `KStream-GlobalKTable Join`: deviceId fora dos trusted_devices |
| **PASSWORD_CHANGE** | Alteração de senha seguida de transação de alto valor | `KStream-KStream Join` (5 min): password_change + tx > R$ 1.000 |
| **ACCOUNT_TAKEOVER** | Sequência completa de invasão | Duplo `KStream-KStream Join` (10 min): login desconhecido → pw change → tx alta |
| **EMPTYING_ACCOUNT** | Tentativa de esvaziar saldo da conta | `aggregate()` + Tumbling Window (60s): ≥ 5 txs de alto valor |
| **SUSPICIOUS_PARALLEL_LOGIN** | Logins simultâneos de dispositivos diferentes | Session Window + Allen overlaps: sessões ativas com devices diferentes sobrepostas |
| **SUSPICIOUS_FARAWAY_LOGIN** | Login de localizações impossivelmente distantes | Processor API + KeyValueStore + Allen before: velocidade > 900 km/h |
| **ACCOUNT_UNDER_OBSERVATION** | Conta com múltiplos alertas de fraude | Tumbling Window + count(): ≥ 3 alertas no mesmo accountId em 2 min |

---

## Tópicos Kafka

| Tópico | Partições | Replicação | Conteúdo |
|--------|-----------|------------|----------|
| `transactions.raw` | 3 | 3 | Eventos de transação (PIX, CRED, DEB) |
| `auth.events` | 3 | 3 | Eventos de autenticação (login, password_change) |
| `fraud.events` | 3 | 3 | Alertas de fraude gerados pelas topologias |
| `clients.profiles` | 1 | 3 | Perfis de clientes (compactado, para GlobalKTable) |

---

## Modelos de Dados

### TransactionEvent

```json
{
  "transactionId": "tx-a1b2c3d4",
  "accountId": "acc-u-000-0",
  "userId": "u-000",
  "type": "PIX",
  "amount": 150.00,
  "deviceId": "dev-u-000-0",
  "ipAddress": "177.10.174.12",
  "destinationAccount": "acc-u-001-0",
  "timestamp": 1715712000000
}
```

> O campo `timestamp` é populado pelos produtores (event-time) para uso correto das janelas temporais no Kafka Streams.

### AuthEvent

```json
{
  "eventId": "auth-e1f2g3h4",
  "userId": "u-000",
  "eventType": "login",
  "deviceId": "dev-u-000-0",
  "ipAddress": "177.10.174.12",
  "latitude": -20.3155,
  "longitude": -40.3128,
  "timestamp": 1715712000000
}
```

### FraudAlert

```json
{
  "alertId": "alert-99a88b",
  "alertType": "ACCOUNT_TAKEOVER",
  "userId": "u-000",
  "accountId": "acc-u-000-0",
  "description": "Account takeover: Suspect login -> Password change -> High value transaction (R$5000.00)",
  "severity": "CRITICAL",
  "timestamp": 1715712000000
}
```

---

## Produtores de Eventos

### LegitimateEventProducer
- Gera eventos normais continuamente
- 70% de chance de transação, 30% de evento de autenticação
- Valores entre R$ 150–200
- Usa apenas dispositivos e IPs confiáveis

### Produtores de Fraude
Cada produtor injeta **um evento malicioso específico**:

| Producer | Comportamento |
|----------|---------------|
| `HighAmountFraudProducer` | Transação única entre R$ 8.000–15.000 |
| `BurstTransactionFraudProducer` | 10 transações rápidas em sequência |
| `UnknownDeviceFraudProducer` | Transação com `deviceId` aleatório (não confiável) |
| `PasswordChangeFraudProducer` | Evento `password_change` após login de dispositivo desconhecido |
| `AccountTakeoverFraudProducer` | Sequência completa: login desconhecido + troca de senha + transação alta |
| `EmptyingAccountFraudProducer` | Múltiplas transações de alto valor seguidas |
| `ParallelLoginFraudProducer` | Logins de SP e Recife em devices不同 (teste) |
| `FarawayLoginFraudProducer` | Login de SP e Tóquio em 500ms (teste) |
| `UnderObservationFraudProducer` | Redispara alertas para o mesmo accountId (teste) |

---

---

## Etapa 2 — Migração para Kafka Streams e CEP (Plano de Implementação)

Esta seção documenta o plano de implementação para a Etapa 2, conforme requisitos do **Projeto2.pdf** e conceitos do arquivo **gambiarra/conceitos.txt**. A implementação substitui **todos os consumidores manuais** por **9 topologias Kafka Streams**, dividida em **7 fases sequenciais**.

---

### Fase 1: Atualizar Modelos de Dados

**Objetivo:** Adicionar campos necessários para as topologias CEP.

| Modelo | Campos a adicionar | Motivo |
|--------|-------------------|--------|
| `AuthEvent.java` | `latitude` (Double), `longitude` (Double) | Necessário para cálculo de distância no `SUSPICIOUS_FARAWAY_LOGIN` |
| `FraudAlert.java` | `alertId` (String, UUID), `accountId` (String), `severity` (String) | Rastreabilidade, associação com conta, priorização |
| `TransactionEvent.java` | `timestamp` (long) | Event-time para janelas temporais do Kafka Streams |
| `ClientProfile.java` | `homeLatitude` (double), `homeLongitude` (double) | Coordenadas geográficas dos clientes |

**Artefatos alterados:**
- `AuthEvent.java` — construtor, getters/setters, equals/hashCode, toString
- `FraudAlert.java` — construtor com geração automática de `alertId`, getters/setters, equals/hashCode, toString
- `TransactionEvent.java` — novo campo `timestamp`, construtor sobrecarregado
- `ClientProfile.java` — novo record com campos de geolocalização
- `ClientGenerator.java` — gerar `homeLatitude` (ex: -15 a -25) e `homeLongitude` (ex: -40 a -50) para o Brasil; **publicar perfis no tópico `clients.profiles`**
- `ClientLoader.java` — ler os novos campos do JSON
- **Todos os produtores** — preencher `timestamp` com `System.currentTimeMillis()` nos eventos

---

### Fase 2: Infraestrutura — pom.xml

**Objetivo:** Adicionar dependências para Kafka Streams e Spring Boot.

**2.1** Adicionar `kafka-streams`:
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>${kafka.version}</version>
</dependency>
```

**2.2** Adicionar Spring Boot Web como parent POM:
```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.2.5</version>
</parent>
```
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>
```

**2.3** Atualizar `KafkaConfig.java`:
- `acks=all` (era "1") — resiliência
- Novo método `streamsProps(String appId)` com:
  - `APPLICATION_ID_CONFIG`
  - `BOOTSTRAP_SERVERS_CONFIG`
  - `DEFAULT_KEY_SERDE_CLASS_CONFIG` = `Serdes.String().getClass()`
  - `DEFAULT_VALUE_SERDE_CLASS_CONFIG` = `JsonSerde.class`
  - `STATE_DIR_CONFIG` = `/tmp/kafka-streams`
  - `COMMIT_INTERVAL_MS_CONFIG` = 100
  - `CACHE_MAX_BYTES_BUFFERING_CONFIG` = 10 MB

**2.4** Adicionar tópico `clients.profiles` no `make topics` (1 partição, compactado)

---

### Fase 3: Serdes para Kafka Streams

**Objetivo:** Criar Serde genérico para que o Kafka Streams serialize/desserialize os modelos automaticamente.

**3.1** Criar `src/main/java/com/frauddetection/serialization/JsonSerde.java`:
- `JsonSerde<T> implements Serde<T>`
- Reusa `JsonSerializer` para serialização
- Cria `JsonDeserializer<T>` interno para desserialização (genérico via `TypeReference`)

---

### Fase 4: Topologias Kafka Streams (9 topologias)

**Objetivo:** Implementar 9 topologias no pacote `com.frauddetection.streams`, substituindo todos os consumidores manuais.

#### 4.0 `FraudDetectionTopology.java` — Classe principal

```java
StreamsBuilder builder = new StreamsBuilder();
registerHighAmount(builder);
registerBurst(builder);
registerUnknownDevice(builder);
registerPasswordChange(builder);
registerAccountTakeover(builder);
registerEmptyingAccount(builder);
registerParallelLogin(builder);
registerFarawayLogin(builder);
registerUnderObservation(builder);
KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
streams.start();
Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
```

#### 4.1 `HighAmountTopology` — Substitui `HighAmountConsumer`

```
transactions.raw
  └── groupBy(accountId)
        └── windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
              └── aggregate(
                    () -> new TxHistory(),
                    (accountId, tx, history) -> {
                        double avg = history.average();
                        if (history.isEmpty() && tx.getAmount() > 8000.0) alert(HIGH_AMOUNT);
                        else if (!history.isEmpty() && tx.getAmount() > avg * 3) alert(HIGH_AMOUNT);
                        history.add(tx);
                        return history;
                    })
sink → fraud.events
```

- Operações: `groupBy()`, `aggregate()` (stateful)
- Janela: Sliding Window (5 min)

#### 4.2 `BurstTopology` — Substitui `BurstTransactionConsumer`

```
transactions.raw
  └── groupBy(accountId)
        └── windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
              └── count()
                    └── filter((key, count) -> count >= 5)
                          └── mapValues(...)
sink → fraud.events (BURST_TRANSACTIONS)
```

- Operações: `groupByKey()`, `count()` (stateful)
- Janela: Tumbling Window (60s)

#### 4.3 `UnknownDeviceTopology` — Substitui `UnknownDeviceConsumer`

Usa `GlobalKTable<String, ClientProfile>` carregada do tópico `clients.profiles`.

```
clients.profiles → GlobalKTable (userId → ClientProfile)

transactions.raw → KStream(userId, TransactionEvent)
  └── join(globalClientsTable,
        (tx, profile) -> profile.getTrustedDevices().contains(tx.getDeviceId()) ? null : tx)
        └── filter(tx -> tx != null)
sink → fraud.events (UNKNOWN_DEVICE)
```

- Operações: `join()` com `GlobalKTable` (stateful)
- Store: GlobalKTable em memória (cópia completa em cada instância)

#### 4.4 `PasswordChangeTopology` — Substitui `PasswordChangeConsumer`

```
auth.events
  ├── filter(eventType == "password_change") → KStream(userId, PwChangeEvent)
  └── filter(eventType == "login") → KStream(userId, LoginEvent) (para pipeline)

transactions.raw → filter(amount > 1000.0) → KStream(userId, TxEvent)

PwChangeStream.join(TxStream,
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
  → filter auth antes de tx (Allen: before)
sink → fraud.events (PASSWORD_CHANGE)
```

- Operações: `filter()` (stateless), `join()` (stateful)
- Janela: JoinWindow (5 min)

#### 4.5 `AccountTakeoverTopology` — Substitui `AccountTakeoverConsumerProducer`

```
auth.events
  ├── filter(eventType == "login" && device não confiável) → KStream(userId, LoginEvent)
  └── filter(eventType == "password_change") → KStream(userId, PwChangeEvent)

LoginStream.join(PwChangeStream,
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)))
  → KStream(userId, MidEvent)

transactions.raw
  └── filter(amount > 1000.0) → KStream(userId, TxEvent)

MidStream.join(TxStream,
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)))
sink → fraud.events (ACCOUNT_TAKEOVER)
```

- Relação Allen: `Login before PwChange before HighTx`
- Operações: `filter()` (stateless), `join()` (stateful)

#### 4.6 `EmptyingAccountTopology` — Substitui `EmptyingAccountConsumerProducer`

```
transactions.raw
  └── filter(amount >= 1000.0)
        └── groupBy(userId)
              └── windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
                    └── count()
                          └── filter((key, count) -> count >= 5)
sink → fraud.events (EMPTYING_ACCOUNT)
```

- Operações: `groupByKey()`, `count()` (stateful)
- Janela: Tumbling Window (60s)

#### 4.7 `ParallelLoginTopology` — Nova Situação 1

```
auth.events
  └── filter(eventType == "login")
        └── groupByKey()
              └── windowedBy(SessionWindows.withInactivityGap(Duration.ofMinutes(5)))
                    └── aggregate(
                          () -> new SessionState(),
                          (userId, auth, state) -> {
                              if (state.hasActiveSession() &&
                                  !state.getDeviceId().equals(auth.getDeviceId()) &&
                                  state.overlapsWith(auth.getTimestamp())) {
                                  emitAlert(SUSPICIOUS_PARALLEL_LOGIN);
                              }
                              state.addSession(auth);
                              return state;
                          })
```

- Relação Allen: `X overlaps Y` ou `X during Y`
- Operações: `filter()` (stateless), `groupByKey()`, `aggregate()` (stateful)
- Janela: Session Window (gap de 5 min)

#### 4.8 `FarawayLoginTopology` — Nova Situação 2

Usa Processor API (custom `Transformer`) com KeyValueStore local (RocksDB).

```
auth.events
  └── filter(eventType == "login")
        └── transform(() -> new FarawayTransformer())

class FarawayTransformer implements Transformer<String, AuthEvent, KeyValue<String, FraudAlert>> {
    KeyValueStore<String, LastLogin> store;

    void init(ProcessorContext context) {
        store = context.getStateStore("faraway-login-store");
    }

    KeyValue<String, FraudAlert> transform(String key, AuthEvent auth) {
        LastLogin last = store.get(key);
        if (last != null) {
            double dist = haversine(last.lat, last.lon, auth.getLatitude(), auth.getLongitude());
            long deltaT = auth.getTimestamp() - last.timestamp;
            double velocidade = dist / (deltaT / 3600.0);
            if (velocidade > 900.0) emitAlert(SUSPICIOUS_FARAWAY_LOGIN);
        }
        store.put(key, new LastLogin(auth.getLatitude(), auth.getLongitude(), auth.getTimestamp()));
        return null;
    }
}
```

- Relação Allen: `X before Y`
- Operações: `filter()` (stateless), `transform()` + `KeyValueStore` (stateful)

#### 4.9 `UnderObservationTopology` — Nova Situação 3

```
fraud.events
  └── filter(alerta existente, não o próprio UNDER_OBSERVATION)
        └── selectKey((k, v) -> v.getAccountId())
              └── groupByKey()
                    └── windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(2)))
                          └── count(Materialized.as("observation-store"))
                                └── filter((key, count) -> count >= 3)
                                      └── mapValues(count -> new FraudAlert("ACCOUNT_UNDER_OBSERVATION", ...))
                                            └── to("fraud.events")
```

- Operações: `filter()`, `selectKey()` (stateless), `groupByKey()`, `count()` (stateful)
- Janela: Tumbling Window (2 min)
- Store: `observation-store` para Interactive Queries

---

### Mapeamento de Conceitos

| Feature | Operações Stateless | Operações Stateful | Tipo de Janela / Store | Relação de Allen |
|---|---|---|---|---|
| **HighAmount** | `filter()`, `mapValues()` | `aggregate()` | Sliding Window (5 min) | — |
| **Burst** | `filter()` | `groupByKey()`, `count()` | Tumbling Window (60s) | — |
| **UnknownDevice** | `filter()` | `join()` (GlobalKTable) | GlobalKTable em memória | — |
| **PasswordChange** | `filter()` | `join()` (KStream-KStream) | JoinWindow (5 min) | $X\ before\ Y$ |
| **AccountTakeover** | `filter()` | `join()` (KStream-KStream) | JoinWindow (10 min) | $X\ before\ Y$ |
| **EmptyingAccount** | `filter()` | `groupByKey()`, `count()` | Tumbling Window (60s) | — |
| **ParallelLogin** | `filter()` | `groupByKey()`, `aggregate()` | Session Window (5 min gap) | $X\ overlaps\ Y$ |
| **FarawayLogin** | `filter()` | `transform()` + `KeyValueStore` | RocksDB Local State Store | $X\ before\ Y$ |
| **UnderObservation** | `filter()`, `selectKey()` | `groupByKey()`, `count()` | Tumbling Window (2 min) | — |

Configuração de caching: `commit.interval.ms=100` e `CACHE_MAX_BYTES_BUFFERING=10MB` para *Event Coalescing* e *Data Deduplication*.

---

### Fase 5: Back-end Spring Boot

**Objetivo:** Criar API REST com SSE para consumo em tempo real dos alertas.

**5.1** `FraudDetectionApplication.java` — classe main Spring Boot:
```java
@SpringBootApplication
public class FraudDetectionApplication {
    public static void main(String[] args) {
        SpringApplication.run(FraudDetectionApplication.class, args);
    }

    @Bean
    public KafkaStreams fraudDetectionStreams() {
        FraudDetectionTopology topology = new FraudDetectionTopology();
        KafkaStreams streams = new KafkaStreams(topology.build(), KafkaConfig.streamsProps("fraud-detection-app"));
        streams.start();
        return streams;
    }
}
```

**5.2** `AlertSSEController.java` — SSE endpoint:
| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/api/alerts/stream` | SSE — stream de alertas em tempo real |

- Usa `SseEmitter` do Spring
- Thread consumidora assina `fraud.events` e faz broadcast

**5.3** `InteractiveQueryController.java` — REST para State Stores:
| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/api/queries/faraway-logins/{userId}` | Último login do usuário no RocksDB |
| `GET` | `/api/queries/observations` | Contas com count >= 3 na observation-store |

- Usa `KafkaStreams.store()` + `ReadOnlyKeyValueStore` para Interactive Queries

---

### Fase 6: Produtores de Teste

**Objetivo:** Criar produtores que disparam as 3 novas situações para validação.

**6.1** `ParallelLoginFraudProducer.java`
- Seleciona um cliente
- Produz login de São Paulo (`lat: -23.5, lng: -46.6`) em device conhecido
- Sleep 2s
- Produz login de Recife (`lat: -8.0, lng: -34.9`) em device diferente
- Session Window de 5 min cobre ambos → overlap temporal → `SUSPICIOUS_PARALLEL_LOGIN`

**6.2** `FarawayLoginFraudProducer.java`
- Seleciona um cliente
- Produz login de São Paulo (`lat: -23.5, lng: -46.6`)
- Sleep 500ms
- Produz login de Tóquio (`lat: 35.7, lng: 139.7`) — ~18.500 km em 500ms → `SUSPICIOUS_FARAWAY_LOGIN`

**6.3** `UnderObservationFraudProducer.java`
- Consumer que lê `fraud.events` e re-produz alertas para o mesmo `accountId`
- Ao atingir 3+ alertas, a topologia UnderObservation emite `ACCOUNT_UNDER_OBSERVATION`

---

### Fase 7: Makefile — Targets Atualizados

**Removidos** (consumidores manuais e tmux não existem mais):
- `detect-high-amount`, `detect-burst`, `detect-unknown-device`, `detect-password-change`, `detect-account-takeover`, `detect-emptying-account`
- `tmux`, `tmux-scaled`, `tmux-kill`

**Modificado**:
- `make clients` — agora também publica perfis no tópico `clients.profiles`
- `make topics` — agora cria também `clients.profiles`

**Adicionados**:
```makefile
run-streams-topology: build        ## Run all 9 Kafka Streams topologies
	java -cp $(JAVA_JAR) com.frauddetection.streams.FraudDetectionTopology

run-web-backend: build             ## Run Spring Boot web backend (port 8080)
	java -cp $(JAVA_JAR) com.frauddetection.web.FraudDetectionApplication

test-parallel-login: build         ## Test suspicious parallel login detection
	java -cp $(JAVA_JAR) com.frauddetection.producers.ParallelLoginFraudProducer

test-faraway-login: build          ## Test impossible faraway login detection
	java -cp $(JAVA_JAR) com.frauddetection.producers.FarawayLoginFraudProducer

test-under-observation: build      ## Test account under observation detection
	java -cp $(JAVA_JAR) com.frauddetection.producers.UnderObservationFraudProducer
```

---

### Dependências entre Fases

```
Fase 1 ──► Fase 2 ──► Fase 3 ──► Fase 4
                                     │
                            ┌────────┴────────┐
                            ▼                 ▼
                        Fase 6            Fase 5
                            │                 │
                            └────────┬────────┘
                                     ▼
                                  Fase 7
```

Cada fase pode ser implementada como uma etapa isolada com validação intermediária (`make build`).

---

### Conceitos Utilizados

| Conceito | Aplicação |
|----------|-----------|
| Kafka Streams DSL | Todas as 9 topologias |
| KStream-KStream Join (Windowed) | PasswordChange, AccountTakeover |
| KStream-GlobalKTable Join | UnknownDevice |
| Session Window | ParallelLogin |
| Sliding Window | HighAmount |
| Tumbling Window | Burst, EmptyingAccount, UnderObservation |
| Processor API + KeyValueStore | FarawayLogin |
| Álgebra de Allen: before | PasswordChange, AccountTakeover, FarawayLogin |
| Álgebra de Allen: overlaps / during | ParallelLogin |
| Stateless: filter, mapValues, selectKey | Todas as topologias |
| Stateful: aggregate, count, join, transform | Todas as topologias |
| Interactive Queries | WebServer (FarawayLogin, UnderObservation) |
| GlobalKTable | UnknownDevice (clientes em memória) |
| RocksDB + Changelog Topics | State Stores internas |
| Event Coalescing / Caching | Config `CACHE_MAX_BYTES_BUFFERING` |
| Producer Acks: all | `KafkaConfig.producerProps()` |
| Event-time | Timestamp dos eventos para janelas |

---

## Exemplos de Fluxo Completo

### Fluxo Completo (Kafka Streams)

```bash
# Terminal 1 — infraestrutura
make up
make topics
make clients
make build

# Terminal 2 — topologia Kafka Streams (9 detectores rodando)
make run-streams-topology

# Terminal 3 — eventos legítimos
make simulate

# Terminal 4 — injetar fraudes
make high-amount
make burst
make unknown-device
make password-change
make account-takeover
make emptying-account

# Terminal 5 — testar novas topologias CEP
make test-parallel-login
make test-faraway-login
make test-under-observation

# Terminal 6 — back-end web (opcional)
make run-web-backend
```

---

## Licença

Este projeto está licenciado sob a licença [Creative Commons Atribuição-CompartilhaIgual 4.0 Internacional (CC BY-SA 4.0)](http://creativecommons.org/licenses/by-sa/4.0/). Você pode compartilhar e adaptar este material, desde que atribua o crédito apropriado e distribua sob a mesma licença.
