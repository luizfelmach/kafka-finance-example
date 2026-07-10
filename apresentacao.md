# Kafka Finance — Fraud Detection System

Detecção de fraudes financeiras em tempo real com Apache Kafka e Kafka Streams

---
---

## O Problema

- Fraudes financeiras causam bilhões em prejuízo anualmente
- Detecção precisa ser **em tempo real** — após a transação confirmada, já é tarde
- Padrões suspeitos variam: valores altos, rajadas, dispositivos desconhecidos, múltiplos logins simultâneos, viagens impossíveis
- Cada tipo de fraude exige uma lógica de detecção diferente (janelas temporais, joins, geolocalização)
- Solução tradicional: consumidores Kafka manuais — código repetitivo, sem janelamento nativo, difícil de escalar

---
---

## A Solução

- **Apache Kafka** — plataforma de eventos para capturar transações e autenticações
- **Kafka Streams** — 9 topologias independentes analisando padrões em paralelo, cada uma com seu próprio `application.id`, grupo de consumidores e state stores
- **Spring Boot** — API REST, SSE para alertas em tempo real, Interactive Queries para consultar state stores
- **Frontend SPA** — dashboard, mapa Leaflet, simulador de fraudes, formulários de login/troca de senha
- **CLI Sources** — produtores de eventos legítimos e fraudulentos para teste

```
 CLI SOURCES ──► Kafka Topics ──► 9 Streams ──► fraud.events ──► Spring Boot ──► Frontend
                                          │
                                    State Stores
                                (Interactive Queries)
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
|---|-----------|---------------|-------|----------------|-------|
| 1 | **HighAmount** | `-high-amount` | `transactions.raw` | filter | Stateless |
| 2 | **Burst** | `-burst` | `transactions.raw` | groupBy + window + count | Tumbling 60s |
| 3 | **UnknownDevice** | `-unknown-device` | `transactions.raw` + profiles | GlobalKTable join | GlobalKTable |
| 4 | **PasswordChange** | `-password-change` | `auth.events` | filter | Stateless |
| 5 | **AccountTakeover** | `-account-takeover` | `auth.events` | filter (failedAttempts) | Stateless |
| 6 | **EmptyingAccount** | `-emptying-account` | `transactions.raw` | groupBy + window + count | Tumbling 60s |
| 7 | **ParallelLogin** | `-parallel-login` | `auth.events` | SessionWindow + Allen overlaps | Session Window |
| 8 | **FarawayLogin** | `-faraway-login` | `auth.events` | Processor API + Haversine | KeyValueStore |
| 9 | **UnderObservation** | `-under-observation` | `fraud.events` | groupBy + window + count | Tumbling 2min |

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

### PasswordChange (severity: HIGH)
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

### AccountTakeover (severity: CRITICAL)
```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_AUTH_EVENTS,
            Consumed.with(Serdes.String(), JsonSerdes.authEvent()))
        .filter((key, auth) -> "password_change".equals(auth.getEventType())
            && auth.getRecentFailedAttempts() != null
            && auth.getRecentFailedAttempts() > 0)
        .mapValues(auth -> FraudAlert.accountTakeover(auth))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    // ... build, start
}
```

AccountTakeover filtra password_change com `recentFailedAttempts > 0` — indicando que houve tentativas frustradas antes da troca de senha. É o alerta mais severo do sistema.

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

## 7. ParallelLogin — Logins Paralelos

**Objetivo:** Detectar sessões simultâneas do mesmo usuário em dispositivos diferentes — um indicador clássico de conta comprometida.

Usa **Session Windows** (gap de 5 minutos) + **Álgebra de Allen** (relação _overlaps_).

```
auth.events
  └── filter(eventType == "login")
        └── groupByKey()
              └── windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                    └── aggregate(SessionState)
                          └── para cada novo login:
                                ├── state.hasActiveSession() AND
                                ├── state.getDeviceId() != auth.getDeviceId() AND
                                └── state.overlapsWith(auth.getTimestamp())
                                      └── → FraudAlert("PARALLEL_LOGIN", severity=HIGH)
                                            └── to("fraud.events")
```

```java
// Pseudocódigo da lógica dentro do aggregate:
// SessionState mantém: activeSession, deviceId, startTime, endTime
// overlapsWith(ts) verifica: startTime <= ts <= endTime
```

---
---

## 8. FarawayLogin — Viagem Impossível

**Objetivo:** Detectar login geograficamente impossível — dois logins consecutivos com distância e velocidade incompatíveis com transporte real.

Usa **Processor API** (`Transformer`) + **KeyValueStore** local (RocksDB) + **Fórmula de Haversine**.

```
auth.events
  └── filter(eventType == "login")
        └── transform(FarawayTransformer)
              │
              ▼
        KeyValueStore("last-login-store")
              │
              ▼
        Haversine(prev.lat, prev.lon, curr.lat, curr.lon)
              │
              ▼
        velocity = distance / (deltaT / 3600.0)
              │
              ▼
        if velocity > 900 km/h:
          └── FraudAlert("FARAWAY_LOGIN", severity=HIGH)
                └── to("fraud.events")
```

```java
public class FarawayTransformer
    implements Transformer<String, AuthEvent, KeyValue<String, FraudAlert>> {

    private KeyValueStore<String, LastLogin> store;

    public void init(ProcessorContext context) {
        store = context.getStateStore("last-login-store");
    }

    public KeyValue<String, FraudAlert> transform(String key, AuthEvent auth) {
        LastLogin last = store.get(key);
        if (last != null) {
            double dist = haversine(last.lat, last.lon,
                auth.getLatitude(), auth.getLongitude());
            long deltaT = auth.getTimestamp() - last.timestamp;
            double speed = deltaT > 0 ? (dist / (deltaT / 3600.0)) : 0;

            if (speed > 900.0) {
                return KeyValue.pair(key,
                    FraudAlert.farawayLogin(auth, dist, speed));
            }
        }
        store.put(key, new LastLogin(auth.getLatitude(),
            auth.getLongitude(), auth.getTimestamp()));
        return null;
    }
}
```

---
---

## 9. UnderObservation — Conta sob Observação

**Objetivo:** Detectar contas que estão sofrendo múltiplos ataques simultâneos — 3+ alertas de qualquer tipo para o mesmo `accountId` em 2 minutos.

Alimenta-se do próprio tópico `fraud.events` (excluindo `UNDER_OBSERVATION` para evitar loop infinito).

```
fraud.events
  └── filter(type != UNDER_OBSERVATION)
        └── selectKey((k,v) -> v.getAccountId())
              └── groupByKey()
                    └── windowedBy(TumblingWindow 2min)
                          └── count(Materialized.as("observation-store"))
                                └── filter(count >= 3)
                                      └── FraudAlert("UNDER_OBSERVATION", severity=LOW)
                                            └── to("fraud.events")
```

```java
public static KafkaStreams build() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(TOPIC_FRAUD_EVENTS,
            Consumed.with(Serdes.String(), JsonSerdes.fraudAlert()))
        .filter((key, alert) -> !"ACCOUNT_UNDER_OBSERVATION"
            .equals(alert.getAlertType()))
        .selectKey((key, alert) -> alert.getAccountId())
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(2)))
        .count(Materialized.as("observation-store"))
        .toStream()
        .filter((key, count) -> count >= 3)
        .mapValues(count -> FraudAlert.underObservation(
            key.key(), null, "Account under observation: " + count + " alerts"))
        .to(TOPIC_FRAUD_EVENTS,
            Produced.with(Serdes.String(), JsonSerdes.fraudAlert()));
    // ... build, start
}
```

Store `observation-store` é queryável via REST para visualizar contas com maior número de alertas.

---
---

## StreamsManager — Gerenciamento Unificado

Spring Boot `@Component` que gerencia todas as 9 topologias dentro da mesma JVM.

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

    public <T> ReadOnlyKeyValueStore<String, T> getStore(String storeName) {
        for (KafkaStreams ks : streams) {
            try {
                return ks.store(StoreQueryParameters.fromNameAndType(
                    storeName, QueryableStoreTypes.keyValueStore()));
            } catch (IllegalArgumentException e) {
                // store not found in this instance
            }
        }
        throw new IllegalArgumentException("Store not found: " + storeName);
    }
}
```

**Vantagens:** desligamento gracioso (`@PreDestroy`), busca unificada de state stores para Interactive Queries.

---
---

## Interactive Queries — REST nas State Stores

```java
@RestController
@RequestMapping("/api/queries")
public class InteractiveQueryController {

    @Autowired
    private StreamsManager streamsManager;

    @GetMapping("/faraway-logins/{userId}")
    public LastLogin getFarawayLogin(@PathVariable String userId) {
        var store = streamsManager.<LastLogin>getStore("last-login-store");
        return store.get(userId);
    }

    @GetMapping("/observations")
    public List<Map<String, Object>> getObservations() {
        var store = streamsManager.<TxHistory>getStore("observation-store");
        List<Map<String, Object>> result = new ArrayList<>();
        try (var iter = store.all()) {
            iter.forEachRemaining(entry -> {
                if (entry.getValue().size() >= 5) {
                    result.add(Map.of("userId", entry.getKey(),
                        "txCount", entry.getValue().size()));
                }
            });
        }
        return result;
    }
}
```

---
---

## Frontend SPA

Aplicação HTML + JS puro servida pelo Spring Boot em `src/main/resources/static/`.

| Página | Rota | Função |
|--------|------|--------|
| Dashboard | `#dashboard` | Stats por severidade + feed recente |
| Live Alerts | `#alerts` | SSE feed + Leaflet map com markers |
| Simulator | `#simulator` | 9 botões de cenário de fraude |
| Create Event | `#create` | Form Transaction + Login/Change Password |
| Queries | `#queries` | Faraway-login e Observation stores |

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

make parallel-login
# → Ver SUSPICIOUS_PARALLEL_LOGIN

make under-observation
# → Ver ACCOUNT_UNDER_OBSERVATION após 3+ alertas
```

---
---

## Para Saber Mais

- Código completo: https://github.com/anomalyco/kafka-finance-example
- Stack: Java 17 + Kafka 3.7.1 + Kafka Streams 3.8 + Spring Boot 3.2.5
- 9 topologias independentes com `application.id` únicos
- Frontend SPA com Leaflet map + SSE + REST
- CLI sources para todos os cenários de fraude

---
---

## Obrigado!

**Perguntas?**
