# Kafka Finance — Fraud Detection System

Detecção de fraudes financeiras em tempo real com Apache Kafka e Kafka Streams

---
---

## O Problema

- Fraudes financeiras causam bilhões em prejuízo anualmente
- Detecção precisa ser **em tempo real** — após a transação confirmada, já é tarde
- Padrões suspeitos variam: valores altos, rajadas, dispositivos desconhecidos, troca de senha após ataque, viagens impossíveis
- Cada tipo de fraude exige uma lógica de detecção diferente (janelas temporais, joins, geolocalização)
- Solução tradicional: consumidores Kafka manuais — código repetitivo, sem janelamento nativo, difícil de escalar

---
---

## A Solução

- **Apache Kafka** — plataforma de eventos para capturar transações e autenticações
- **Kafka Streams** — 7 topologias independentes analisando padrões em paralelo, cada uma com seu próprio `application.id`, grupo de consumidores e state stores
- **Spring Boot** — API REST, SSE para alertas em tempo real
- **Frontend SPA** — dashboard, mapa Leaflet, simulador de fraudes, formulários de login/troca de senha
- **CLI Sources** — produtores de eventos legítimos e fraudulentos para teste

```
 CLI SOURCES ──► Kafka Topics ──► 7 Streams ──► fraud.events ──► Spring Boot ──► Frontend
```

---
---

## Stack Tecnológica

| Tecnologia | Versão | Função |
|-----------|--------|--------|
| Java | 17 | Linguagem principal |
| Apache Kafka | 3.7.1 | Streaming de eventos |
| Kafka Streams | 3.8.0 | Processamento CEP |
| Spring Boot | 3.2.5 | REST + SSE + static files |
| Docker Compose | — | Cluster 3 brokers KRaft |
| Jackson | 2.17 | Serialização JSON |
| Leaflet.js | 1.9.4 | Mapa interativo (OpenStreetMap) |

---
---

## Arquitetura Global

### Fluxo de Dados

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              GERADORES                                   │
│                                                                          │
│  LegitimateEventProducer  (loop contínuo: ~70% tx, ~30% auth)           │
│  CLI Fraud Producers      (1 disparo: high-amount, burst, takeover...)   │
│  REST API (EventController → POST /api/events/*)  (browser/curl)        │
└──────┬──────────────────┬───────────────────────────────────────────────┘
       │                  │
       ▼                  ▼
  transactions.raw    auth.events        clients.profiles (1p, RF=3, compacted)
  (3 partições,       (3 partições,
   RF=3)               RF=3)
       │                  │
       └─────┬────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     7 TOPOLOGIAS KAFKA STREAMS                          │
│                        (cada uma com application.id único)               │
│                                                                         │
│  ┌──────────────┐  ┌──────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  HighAmount   │  │  Burst   │  │ UnknownDevice │  │PasswordChange │  │
│  │  (stateless)  │  │(tumbling │  │(GlobalKTable  │  │  (stateless)  │  │
│  │ filter >50k   │  │  window  │  │    join)      │  │filter pw_chn  │  │
│  │               │  │  5min)   │  │              │  │               │  │
│  └──────┬───────┘  └────┬─────┘  └──────┬───────┘  └───────┬───────┘  │
│         │               │               │                  │           │
│  ┌──────▼─────────┐  ┌─▼───────────┐  ┌▼───────────────┐  │           │
│  │AccountTakeover │  │  Emptying   │  │  FarawayLogin   │  │           │
│  │ (KTable join + │  │   Account   │  │  (aggregate +   │  │           │
│  │ Allen Before)  │  │  (sliding   │  │   Haversine)    │  │           │
│  │                │  │  window 10m)│  │                 │  │           │
│  └──────┬─────────┘  └─────┬───────┘  └────────┬───────┘  │           │
│         │                  │                    │           │           │
└─────────┼──────────────────┼────────────────────┼───────────┘           │
          │                  │                    │                        │
          ▼                  ▼                    ▼                        ▼
     fraud.events (3 partições, RF=3)
          │
          ▼
  AlertSSEController (group.id: "fraud-alert-sse", auto.offset.reset: earliest)
          │
          ▼
  Browser → alerts.js → Leaflet.js (mapa) + feed de alertas
```

### Tópicos e Partições

| Tópico | Partições | RF | Cleanup Policy | Produzido por | Consumido por |
|--------|-----------|----|---------------|---------------|---------------|
| `transactions.raw` | **3** | **3** | delete (padrão) | CLI fraud producers, REST API, LegitimateEventProducer | HighAmount, Burst, EmptyingAccount, AccountTakeover, UnknownDevice |
| `auth.events` | **3** | **3** | delete | CLI fraud producers, REST API, LegitimateEventProducer | PasswordChange, AccountTakeover, FarawayLogin, UnknownDevice |
| `fraud.events` | **3** | **3** | delete | Todas as 7 topologias | AlertSSEController (SSE → frontend) |
| `clients.profiles` | **1** | **3** | **compact** | ClientGenerator | UnknownDevice (GlobalKTable) |

> Todas as mensagens são chaveadas por `userId` → mesmo usuário sempre na mesma partição (co-partição garantida).

### State Stores

| Store | Topologia | Tipo | Chave | Valor |
|-------|-----------|------|-------|-------|
| `takeover-store` | AccountTakeover | Persistent KeyValue | String (userId) | `TakeoverState` (loginSeen, pwChangeSeen, loginTimestamp, pwChangeTimestamp) |
| `last-login-store` | FarawayLogin | Persistent KeyValue | String (userId) | `LoginPair` (previous + current AuthEvent) |
| *(auto-named)* | Burst | Windowed count | Windowed\<String\> | Long |
| *(auto-named)* | EmptyingAccount | Windowed aggregate | Windowed\<String\> | `AccountAggregate` |
| *(GlobalKTable)* | UnknownDevice | Global state (replicado) | String (userId) | `ClientProfile` |

---
---



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

### AuthEvent
```json
{
  "eventId": "auth-e1f2g3h4",
  "userId": "u-000",
  "eventType": "login",
  "deviceId": "dev-u-000-0",
  "ipAddress": "177.10.174.12",
  "latitude": -20.3,
  "longitude": -40.3,
  "timestamp": 1715712000000
}
```

### FraudAlert
```json
{
  "alertId": "alert-99a88b",
  "alertType": "ACCOUNT_TAKEOVER",
  "severity": "CRITICAL",
  "userId": "u-000",
  "accountId": "acc-u-000-0",
  "description": "Password change after 3 failed attempts",
  "timestamp": 1715712000000,
  "latitude": -20.3,
  "longitude": -40.3
}
```

---
---

## Topologias — Visão Geral

Cada topologia é um `KafkaStreams` independente com `application.id`, grupo de consumidores e state stores próprios.

| # | Topologia | app.id suffix | Input | Operações Chave | State |
|--|-----------|---------------|-------|----------------|-------|
| 1 | **HighAmount** | `-high-amount` | `transactions.raw` | filter | Stateless |
| 2 | **Burst** | `-burst` | `transactions.raw` | groupBy + window + count | Tumbling 5min |
| 3 | **UnknownDevice** | `-unknown-device` | `transactions.raw` + profiles | GlobalKTable join | GlobalKTable |
| 4 | **PasswordChange** | `-password-change` | `auth.events` | filter | Stateless |
| 5 | **AccountTakeover** | `-account-takeover` | `auth.events` + `transactions.raw` | KTable aggregate + KStream-KTable join | KeyValueStore |
| 6 | **EmptyingAccount** | `-emptying-account` | `transactions.raw` | groupBy + window + aggregate | Sliding 10min |
| 7 | **FarawayLogin** | `-faraway-login` | `auth.events` | groupByKey + aggregate + flatMap + Haversine | KeyValueStore |

---
---

## Detalhes das Topologias

| # | Topologia | app.id | Inputs | DSL | State Store | Joins | Output |
|---|-----------|--------|--------|-----|-------------|-------|--------|
| 1 | **HighAmount** | `fraud-detection-high-amount` | `transactions.raw` | `filter → mapValues` | ❌ Nenhum | ❌ | `fraud.events` |
| 2 | **Burst** | `fraud-detection-burst` | `transactions.raw` | `groupByKey → windowedBy(5min) → count → filter → map` | Implícito (count) | ❌ | `fraud.events` |
| 3 | **UnknownDevice** | `fraud-detection-unknown-device` | `transactions.raw` + `clients.profiles` | `join(globalTable) → filter → mapValues` | GlobalKTable | KStream-GlobalKTable (`auth.userId` → profile) | `fraud.events` |
| 4 | **PasswordChange** | `fraud-detection-password-change` | `auth.events` | `filter → mapValues` | ❌ Nenhum | ❌ | `fraud.events` |
| 5 | **AccountTakeover** | `fraud-detection-account-takeover` | `auth.events` + `transactions.raw` | `groupByKey → aggregate(TakeoverState) → KTable + join(filter ≥5000)` | `takeover-store` (Persistent KV) | KStream-KTable (userId) | `fraud.events` |
| 6 | **EmptyingAccount** | `fraud-detection-emptying-account` | `transactions.raw` | `groupByKey → windowedBy(sliding 10min) → aggregate → filter → map` | Implícito (aggregate) | ❌ | `fraud.events` |
| 7 | **FarawayLogin** | `fraud-detection-faraway-login` | `auth.events` | `groupByKey → aggregate(LoginPair) → flatMap(Haversine)` | `last-login-store` (Persistent KV) | ❌ | `fraud.events` |

---
---

## 1. HighAmount — Transação de Alto Valor

**Objetivo:** Detectar transações únicas com valor muito acima do normal.

```
transactions.raw
  └── filter(amount > 50000.00)
        └── map → FraudAlert("HIGH_VALUE_TRANSACTION", severity=HIGH)
              └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_TRANSACTIONS_RAW,
            Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
        .filter((key, tx) -> tx.getAmount() > LIMIT)
        .mapValues(tx -> FraudAlert.highValue(tx))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

    KafkaStreams streams = new KafkaStreams(builder.build(),
        KafkaConfig.streamsProps("fraud-detection-high-amount"));
    streams.start();
    return streams;
}
```

---
---

## 2. Burst — Rajada de Transações

**Objetivo:** Detectar 5+ transações em 5 minutos para a mesma conta.

```
transactions.raw
  └── groupByKey()
        └── windowedBy(TumblingWindow 5min)
              └── count()
                    └── filter((key, count) -> count >= 5)
                          └── map → FraudAlert("TRANSACTION_BURST", severity=MEDIUM)
                                └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_TRANSACTIONS_RAW,
            Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
        .count()
        .toStream()
        .filter((windowedKey, count) -> count >= 5)
        .map((windowedKey, count) -> KeyValue.pair(
            windowedKey.key(),
            FraudAlert.transactionBurst(windowedKey.key(), count,
                windowedKey.window().start(), windowedKey.window().end())))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

    KafkaStreams streams = new KafkaStreams(builder.build(),
        KafkaConfig.streamsProps("fraud-detection-burst"));
    streams.start();
    return streams;
}
```

---
---

## 3. UnknownDevice — Dispositivo Desconhecido

**Objetivo:** Detectar transações vindas de dispositivos não cadastrados no perfil do cliente.

Usa `GlobalKTable` para ter uma cópia completa de todos os perfis em memória em cada instância.

```
clients.profiles ──► GlobalKTable(key=userId, value=ClientProfile)
transactions.raw ──► KStream(key=userId, value=TransactionEvent)
                         │
                         ▼
                   join(globalTable)
                         │
                         ▼
              filter(!trustedDevices.contains(deviceId))
                         │
                         ▼
              FraudAlert("UNKNOWN_DEVICE", severity=MEDIUM)
                         │
                         ▼
                   to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();

    GlobalKTable<String, ClientProfile> profiles = builder.globalTable(
        TOPIC_CLIENTS_PROFILES,
        Consumed.with(Serdes.String(), JsonSerdes.clientProfile()));

    builder
        .stream(TOPIC_TRANSACTIONS_RAW,
            Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
        .join(profiles, (tx, profile) -> profile,
            (tx, profile) -> profile.getTrustedDevices()
                .contains(tx.getDeviceId()) ? null : tx)
        .filter((key, tx) -> tx != null)
        .mapValues(tx -> FraudAlert.unknownDevice(
            new AuthEvent(null, tx.getUserId(), null, tx.getDeviceId(),
                tx.getIpAddress(), 0, 0, tx.getTimestamp())))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));

    // ... build, start, return KafkaStreams
}
```

---
---

## 4. PasswordChange — Troca de Senha

**Objetivo:** Detectar eventos de troca de senha (severity LOW, sem marker no mapa).

```
auth.events
  └── filter(eventType == "password_change")
        └── map → FraudAlert("PASSWORD_CHANGE", severity=LOW)
              └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_AUTH_EVENTS,
            Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
        .filter((key, auth) -> "password_change".equals(auth.getEventType()))
        .mapValues(auth -> FraudAlert.passwordChange(auth))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    // ... build, start
}
```

---
---

## 5. AccountTakeover — Álgebra de Allen

**Objetivo:** Detectar account takeover: login de dispositivo desconhecido → troca de senha → transação de alto valor (severity CRITICAL).

Usa **KTable aggregate** + **KStream-KTable join** + **Álgebra de Intervalos de Allen** para verificar a ordem temporal dos eventos.

### Allen's Before Relation

Na Álgebra de Allen, um intervalo X está **Before** de Y se `X.end < Y.start`. Como nossos eventos são pontuais `[t, t]`:

```
login [t₁, t₁]  Before  pwChange [t₂, t₂]  →  t₁ < t₂
pwChange [t₂, t₂]  Before  transaction [t₃, t₃]  →  t₂ < t₃
```

Isso elimina falsos positivos onde a transação aconteceu **antes** do login suspeito.

```
auth.events
  └── groupByKey()
        └── aggregate(TakeoverState)
              ├── loginSeen + loginTimestamp
              ├── pwChangeSeen + pwChangeTimestamp
              └── KTable("takeover-store")

transactions.raw
  └── filter(amount >= 5000)
        └── join(ktable)
              └── filter: loginSeen && pwChangeSeen
                    && loginTimestamp < pwChangeTimestamp   ← Allen Before
                    && pwChangeTimestamp < tx.getTimestamp() ← Allen Before
                          └── FraudAlert("ACCOUNT_TAKEOVER", severity=CRITICAL)
                                └── to("fraud.events")
```

```java
KTable<String, TakeoverState> authState = builder
    .stream(KafkaConfig.TOPIC_AUTH_EVENTS,
        Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
    .groupByKey()
    .aggregate(
        TakeoverState::new,
        (key, auth, state) -> {
            if ("login".equals(auth.getEventType())) {
                state.setLoginSeen(true);
                state.setLoginTimestamp(auth.getTimestamp());    // ← Allen
            }
            if ("password_change".equals(auth.getEventType())) {
                state.setPwChangeSeen(true);
                state.setPwChangeTimestamp(auth.getTimestamp()); // ← Allen
            }
            return state;
        },
        Materialized.<String, TakeoverState>as(Stores.persistentKeyValueStore("takeover-store"))
            .withKeySerde(Serdes.String())
            .withValueSerde(JsonSerdes.takeoverState())
    );

builder
    .stream(KafkaConfig.TOPIC_TRANSACTIONS_RAW,
        Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
    .filter((key, tx) -> tx.getAmount() >= 5000)
    .join(authState, (tx, state) -> {
        if (state.isLoginSeen() && state.isPwChangeSeen()
            && state.getLoginTimestamp() < state.getPwChangeTimestamp()   // Allen Before
            && state.getPwChangeTimestamp() < tx.getTimestamp()) {        // Allen Before
            return FraudAlert.accountTakeover(tx);
        }
        return null;
    })
    .filter((key, alert) -> alert != null)
    .to(KafkaConfig.TOPIC_FRAUD_EVENTS,
        Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
```

### Exemplo

| Evento | Timestamp | Allen Before |
|--------|-----------|--------------|
| Login dispositivo desconhecido | 10:00:00 | — |
| Troca de senha | 10:00:05 | login **Before** pwChange ✅ (10:00:00 < 10:00:05) |
| Transação R$ 5.500 | 10:00:10 | pwChange **Before** tx ✅ (10:00:05 < 10:00:10) |

**Alerta gerado** ✅

| Evento | Timestamp |
|--------|-----------|
| Transação R$ 5.500 | 10:00:00 |
| Login dispositivo desconhecido | 10:00:05 |
| Troca de senha | 10:00:10 |

Login **NÃO é Before** tx (10:00:05 > 10:00:00) → **Alerta suprimido** ✅

### ▶️ Demo
```bash
make account-takeover
```

---
---

## 6. EmptyingAccount — Esvaziamento de Conta

**Objetivo:** Detectar 3+ transações em 10 minutos cujo saldo total ultrapasse -R$1.000, indicando tentativa de esvaziar a conta.

Usa **SlidingWindows** (janela deslizante) para capturar transações próximas independentemente do horário exato de início.

```
transactions.raw
  └── groupByKey()
        └── windowedBy(SlidingWindows 10min)
              └── aggregate(AccountAggregate)
                    ├── totalAmount (subtrai cada valor)
                    ├── count
                    └── lastUserId / lastAccountId
                          └── filter(totalAmount < -1000 && count >= 3)
                                └── FraudAlert("EMPTYING_ACCOUNT", severity=HIGH)
                                      └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_TRANSACTIONS_RAW,
            Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
        .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.transactionEvent()))
        .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(
            Duration.ofMinutes(10), Duration.ofMinutes(2)))
        .aggregate(
            AccountAggregate::new,
            (key, tx, agg) -> agg.add(tx),
            Materialized.with(Serdes.String(), JsonSerdes.accountAggregate()))
        .toStream()
        .filter((windowedKey, agg) ->
            agg.getTotalAmount().compareTo(new BigDecimal("-1000")) < 0
            && agg.getCount() >= 3)
        .map((windowedKey, agg) -> KeyValue.pair(
            windowedKey.key(),
            FraudAlert.emptyingAccount(
                agg.getLastAccountId(), agg.getLastUserId(),
                "Account drained: account=" + agg.getLastAccountId()
                + " | user=" + agg.getLastUserId()
                + " | balance=R$" + agg.getTotalAmount()
                + " in " + agg.getCount() + " transactions")))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    // ... build, start
}
```

### ▶️ Demo
```bash
make emptying-account
```

---
---

## 7. FarawayLogin — Viagem Impossível

**Objetivo:** Detectar login geograficamente impossível — dois logins consecutivos com distância e velocidade incompatíveis com transporte real.

Usa **groupByKey + aggregate** (LoginPair POJO) + **toStream + flatMap** + **Fórmula de Haversine** — puro DSL, sem Processor API.

```
auth.events
  └── filter(eventType == "login")
        └── groupByKey()
              └── aggregate(LoginPair)
                    └── KeyValueStore("last-login-store")

LoginPair mantém: previous (AuthEvent) e current (AuthEvent)

toStream + flatMap:
  ├── previous == null? → apenas atualiza, não emite alerta
  └── previous != null?
        ├── Haversine(prev.lat, prev.lon, curr.lat, curr.lon)
        ├── speed = distance / (deltaT / 3600.0)
        └── speed > 900 km/h?
              └── FraudAlert("FARAWAY_LOGIN", severity=HIGH)
                    └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();

    builder
        .stream(TOPIC_AUTH_EVENTS,
            Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
        .filter((key, auth) -> "login".equals(auth.getEventType()))
        .groupByKey()
        .aggregate(LoginPair::new,
            (key, auth, pair) -> pair.add(auth),
            Materialized.<String, LoginPair, KeyValueStore<Bytes, byte[]>>
                as("last-login-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerdes.loginPair()))
        .toStream()
        .flatMap((key, pair) -> {
            List<KeyValue<String, FraudAlert>> results = new ArrayList<>();
            if (pair.getPrevious() != null) {
                double dist = haversine(
                    pair.getPrevious().getLatitude(), pair.getPrevious().getLongitude(),
                    pair.getCurrent().getLatitude(), pair.getCurrent().getLongitude());
                long deltaT = pair.getCurrent().getTimestamp()
                    - pair.getPrevious().getTimestamp();
                double speed = deltaT > 0 ? (dist / (deltaT / 3600.0)) : 0;
                if (speed > 900.0) {
                    results.add(KeyValue.pair(key,
                        FraudAlert.farawayLogin(pair.getCurrent(), dist, speed)));
                }
            }
            return results;
        })
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    // ... build, start
}
```

---
---

## StreamsManager — Gerenciamento Unificado

Spring Boot `@Component` que gerencia todas as 7 topologias dentro da mesma JVM.

```java
@Component
public class StreamsManager {

    private List<KafkaStreams> streams;

    @PostConstruct
    public void start() {
        streams = FraudDetectionTopology.buildAll();
    }

    @PreDestroy
    public void shutdown() {
        streams.forEach(KafkaStreams::close);
    }
}
```

**Vantagens:** desligamento gracioso (`@PreDestroy`), todas as topologias em uma única JVM.

---
---

## Escalabilidade Horizontal

Cada topologia já é uma aplicação Kafka Streams independente com `application.id` único. Isso significa que Kafka Streams as trata como **consumer groups independentes** — cada uma tem seus próprios offsets e consome todas as mensagens dos tópicos de entrada.

### Cenário atual (JVM única)

```
┌─────────────────────────────────────────────┐
│              JVM Única (Spring Boot)         │
│                                             │
│  ┌──────────┐ ┌─────────┐ ┌──────────────┐ │
│  │HighAmount│ │  Burst  │ │AccountTakeover│ │
│  └──────────┘ └─────────┘ └──────────────┘ │
│  ┌──────────┐ ┌─────────┐ ┌──────────────┐ │
│  │  Emptying│ │Faraway..│ │ UnknownDevice│ │
│  └──────────┘ └─────────┘ └──────────────┘ │
│  ┌──────────┐                               │
│  │Password..│                               │
│  └──────────┘                               │
└─────────────────────────────────────────────┘
```

### Separado (N JVMs)

```
┌────────────┐  ┌────────────┐  ┌────────────┐
│ JVM 1      │  │ JVM 2      │  │ JVM 3      │
│ HighAmount │  │ AccountTko │  │ Burst      │
│ (stateless)│  │ (stateful) │  │ (windowed) │
└────────────┘  └────────────┘  └────────────┘
```

| Aspecto | JVM única | N JVMs separadas |
|---------|-----------|-------------------|
| **Isolamento** | Falha em uma topologia derruba todas | Falha isolada por topologia |
| **Recursos** | Todas competem por CPU/memória/heap | Cada uma com recursos dedicados |
| **Escala** | Vertical (aumentar a JVM) | Horizontal (replicar topologias pesadas) |
| **Deploy** | Um deploy para tudo | Deploy independente por topologia |

### Como fazer na prática

O código **não precisa mudar**. Basta extrair cada `build()` para sua própria `main()`:

```java
// Exemplo: HighAmountTopology standalone
public class HighAmountTopology {
    public static void main(String[] args) {
        KafkaStreams streams = build();  // mesma DSL de hoje
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

E cada uma vira um container Docker separado com seu próprio entry point.

---
---

## Frontend SPA

Aplicação HTML + JS puro servida pelo Spring Boot em `src/main/resources/static/`.

| Página | Rota | Função |
|--------|------|--------|
| Dashboard | `#dashboard` | Stats por severidade + feed recente |
| Live Alerts | `#alerts` | SSE feed + Leaflet map com markers |
| Simulator | `#simulator` | 7 botões de cenário de fraude |
| Create Event | `#create` | Form Transaction + Login/Change Password |

### Leaflet Map

- CircleMarkers coloridos por severidade (CRITICAL=red, HIGH=amber, MEDIUM=blue, LOW=green)
- Popup com alert type, descrição e timestamp
- Centralizado no Brasil (zoom 4)
- TileLayer do OpenStreetMap

### Login UI

Campos simulados: username + password + "Remember this device". Se desmarcado, device é aleatório → gera alerta `UNKNOWN_DEVICE`.

---
---

## Demonstração ao Vivo

### 🖥️ Slide: Infraestrutura
```bash
# Terminal 1 — Iniciar cluster Kafka (3 brokers KRaft)
make up
make topics
make clients
make build
make spring-boot

# Browser → http://localhost:8080
```

### 🖥️ Slide: Tráfego Normal
```bash
# Opção 1: CLI
make simulate

# Opção 2: Dashboard (toggle "Simular" no nav bar)
# → Gera ~70% transações, ~30% auth events em loop
```

### ▶️ Slide: HighAmount (filter > R$ 50k)
```bash
make high-amount
# → Alerta HIGH_VALUE_TRANSACTION no dashboard
```

### ▶️ Slide: Burst (tumbling window 5min, ≥5 txs)
```bash
make burst
# → Alerta TRANSACTION_BURST (MEDIUM)
```

### ▶️ Slide: UnknownDevice (GlobalKTable join)
```bash
make unknown-device
# → Alerta UNKNOWN_DEVICE + marker no mapa
```

### ▶️ Slide: PasswordChange (stateless filter)
```bash
make password-change
# → Alerta PASSWORD_CHANGE (LOW)
```

### ▶️ Slide: AccountTakeover (Allen's Before)
```bash
make account-takeover
# → Alerta CRITICAL com sequência temporal verificada
# → login Before pwChange Before transaction
```

### ▶️ Slide: EmptyingAccount (sliding window 10min)
```bash
make emptying-account
# → Alerta EMPTYING_ACCOUNT (HIGH)
```

### ▶️ Slide: FarawayLogin (Haversine > 900 km/h)
```bash
make faraway-login
# → Marker SP → Tokyo no mapa
# → Velocidade ~133.000 km/h
```

---
---

## Para Saber Mais

- Código completo: https://github.com/anomalyco/kafka-finance-example
- Stack: Java 17 + Kafka 3.7.1 + Kafka Streams 3.8 + Spring Boot 3.2.5
- 7 topologias independentes com `application.id` únicos
- Frontend SPA com Leaflet map + SSE + REST
- CLI sources para todos os cenários de fraude

---
---

## Obrigado!

**Perguntas?**
