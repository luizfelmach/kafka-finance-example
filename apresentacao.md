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
| 2 | **Burst** | `-burst` | `transactions.raw` | groupBy + window + count | Tumbling 60s |
| 3 | **UnknownDevice** | `-unknown-device` | `transactions.raw` + profiles | GlobalKTable join | GlobalKTable |
| 4 | **PasswordChange** | `-password-change` | `auth.events` | filter | Stateless |
| 5 | **AccountTakeover** | `-account-takeover` | `auth.events` + `transactions.raw` | KTable aggregate + KStream-KTable join | KeyValueStore |
| 6 | **EmptyingAccount** | `-emptying-account` | `transactions.raw` | groupBy + window + count | Tumbling 60s |
| 7 | **FarawayLogin** | `-faraway-login` | `auth.events` | groupByKey + aggregate + flatMap + Haversine | KeyValueStore |

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

**Objetivo:** Detectar 5+ transações em 60 segundos para a mesma conta.

```
transactions.raw
  └── groupByKey()
        └── windowedBy(TumblingWindow.of(Duration.ofSeconds(60)))
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
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
        .count()
        .toStream()
        .filter((key, count) -> count >= 5)
        .map((key, count) -> KeyValue.pair(key.key(),
            FraudAlert.transactionBurst(key.key(), count, 0, 0)))
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

## 4-5. PasswordChange + AccountTakeover — Eventos de Autenticação

### PasswordChange (severity: LOW, sem marker no mapa)
```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_AUTH_EVENTS,
            Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
        .filter((key, auth) -> "password_change".equals(auth.getEventType()))
        .mapValues(auth -> FraudAlert.passwordChange(auth))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), Serdes.String()));
    // ... build, start
}
```

### AccountTakeover (severity: CRITICAL)

Usa **KTable aggregate** + **KStream-KTable join** para correlacionar eventos de autenticação com transações de alto valor:

```
auth.events
  └── filter: login ou password_change
        └── groupByKey()
              └── aggregate(TakeoverState)
                    └── KTable: acompanha loginSeen e pwChangeSeen por usuário

transactions.raw
  └── filter: amount >= 5000
        └── join(ktable)
              └── filter: loginSeen && pwChangeSeen
                    └── FraudAlert("ACCOUNT_TAKEOVER")
                          └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();

    // KTable: estado de autenticação por usuário
    KTable<String, TakeoverState> takeoverState = builder
        .stream(TOPIC_AUTH_EVENTS,
            Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
        .filter((key, auth) -> "login".equals(auth.getEventType())
            || "password_change".equals(auth.getEventType()))
        .groupByKey()
        .aggregate(TakeoverState::new,
            (key, auth, state) -> state.update(auth),
            Materialized.<String, TakeoverState, KeyValueStore<Bytes, byte[]>>
                as("takeover-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(JsonSerdes.takeoverState()));

    // KStream-KTable join: transações >= 5000 com takeoverState
    builder
        .stream(TOPIC_TRANSACTIONS_RAW,
            Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
        .filter((key, tx) -> tx.getAmount() >= 5000.0)
        .join(takeoverState, (tx, state) -> state,
            (tx, state) -> {
                if (state != null && state.isLoginSeen() && state.isPwChangeSeen()) {
                    return FraudAlert.accountTakeover(tx);
                }
                return null;
            })
        .filter((key, alert) -> alert != null)
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    // ... build, start
}
```

AccountTakeover correlaciona login + troca de senha com transação de alto valor (≥ R$5.000).Alertas duplicados para o mesmo usuário são aceitos (não há filtro de alerta já enviado).

---
---

## 6. EmptyingAccount — Esvaziamento de Conta

**Objetivo:** Detectar 5+ transações de alto valor (≥ R$1.000) em 60 segundos, indicando tentativa de esvaziar a conta.

```
transactions.raw
  └── filter(amount >= 1000.0)
        └── groupByKey()
              └── windowedBy(TumblingWindow 60s)
                    └── aggregate(AccountAggregate)
                          └── filter(count >= 5)
                                └── FraudAlert("EMPTYING_ACCOUNT", severity=HIGH)
                                      └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_TRANSACTIONS_RAW,
            Consumed.with(Serdes.String(), JsonSerdes.transactionEvent()))
        .filter((key, tx) -> tx.getAmount() >= 1000.0)
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60)))
        .aggregate(AccountAggregate::new,
            (key, tx, agg) -> { agg.add(tx); return agg; },
            Materialized.with(Serdes.String(), JsonSerdes.accountAggregate()))
        .toStream()
        .filter((key, agg) -> agg.size() >= 5)
        .map((key, agg) -> KeyValue.pair(key.key(),
            FraudAlert.emptyingAccount(key.key(), key.key(),
                agg.size() + " high-value transactions in 60s")))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    // ... build, start
}
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

```bash
# Terminal 1 — Infraestrutura
make up
make topics
make clients
make build
make spring-boot

# Browser → http://localhost:8080

# Terminal 2 — Tráfego legítimo
make simulate

# Terminal 3 — Fraudes para testar
make faraway-login
# → Ver alerta no feed + marker no mapa (SP → Tokyo)

make account-takeover
# → Ver CRITICAL no dashboard

make high-amount
# → Ver HIGH_VALUE_TRANSACTION

make emptying-account
# → Ver EMPTYING_ACCOUNT no feed
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
