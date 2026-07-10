# Kafka Finance Example — Fraud Detection System

Real-time financial fraud detection using **Apache Kafka** as the event streaming platform and **Kafka Streams** as the CEP (Complex Event Processing) engine. The project simulates bank transactions and authentication events, detecting suspicious patterns through 7 independent Kafka Streams topologies using temporal windows, stateful aggregations, stream-table joins, and geolocation analysis.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Setup & Execution](#setup--execution)
- [Make Targets](#make-targets)
- [Data Models](#data-models)
- [Topologies](#topologies)
- [REST API](#rest-api)
- [Frontend](#frontend)
- [CLI Sources](#cli-sources)
- [License](#license)

---

## Overview

1. **Client profiles** — 100 simulated bank clients with accounts, trusted devices, IPs, and home coordinates.
2. **Legitimate events** — continuous stream of normal transactions and logins.
3. **Fraud simulation** — specialized CLI sources inject malicious behaviour (high-value txs, unknown devices, impossible travel, etc.).
4. **Real-time detection** — 7 independent Kafka Streams topologies analyse patterns using the full Kafka Streams DSL: filters, windowed counts, GlobalKTable joins, KTable aggregations, `groupByKey().aggregate()`, `toStream().flatMap()`, and `KStream-KTable` joins.
5. **Frontend SPA** — real-time dashboard with Leaflet map, SSE live feed, and fraud simulator.

---

## Architecture

```
                        CLI SOURCES                          REST API
 ┌─────────────────┐  ┌──────────────────────────────────┐  ┌─────────────────┐
 │ LegitimateEvent │  │ Fraud Sources (7 types)           │  │ POST /api/events│
 │   Producer      │  │ high-amount, burst, unknown-device│  │ /transaction    │
 └────────┬────────┘  │ password-change, account-takeover,│  │ /auth           │
          │           │ emptying-account, faraway-login  │  └────────┬────────┘
          │           └────────┬────────┬────────┬────────┘           │
          ▼                    ▼        ▼        ▼                    ▼
 ┌──────────────┐   ┌──────────────┐   ┌────────────────┐   ┌──────────────────┐
 │ transactions │   │  auth.events │   │  fraud.events  │   │ clients.profiles │
 │    .raw      │   │              │   │                │   │   (compactado)   │
 │ (3p / RF=3)  │   │ (3p / RF=3)  │   │ (3p / RF=3)    │   │   (1p / RF=3)    │
 └──────┬───────┘   └──────┬───────┘   └───────┬────────┘   └────────┬─────────┘
        │                  │                   │                     │
        └──────────────────┼───────────────────┼─────────────────────┘
                           ▼                   ▼
              ┌──────────────────────────────────────────────────────┐
              │          KAFKA STREAMS (7 independent instances)      │
              │                                                      │
              │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │
              │  │ HighAmount   │ │ Burst        │ │ UnknownDevice│  │
              │  │ app.id=...   │ │ app.id=...   │ │ app.id=...   │  │
              │  │ -high-amount │ │ -burst       │ │ -unknown-dev.│  │
              │  └──────────────┘ └──────────────┘ └──────────────┘  │
              │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │
              │  │ PasswordChg  │ │ AcctTakeover │ │ EmptyingAcct │  │
              │  │ app.id=...   │ │ app.id=...   │ │ app.id=...   │  │
              │  │ -password-c. │ │ -account-t.  │ │ -emptying-a. │  │
              │  └──────────────┘ └──────────────┘ └──────────────┘  │
              │  ┌──────────────┐                                     │
              │  │ FarawayLogin │                                     │
              │  │ app.id=...   │                                     │
              │  │ -faraway-l.  │                                     │
              │  └──────────────┘                                     │
              │                                                      │
              │  State Stores: last-login-store, takeover-store      │
              └──────────────────────┬───────────────────────────────┘
                                     │
                                     ▼ (alerts)
                              ┌──────────────┐
                              │ fraud.events │
                              └──────┬───────┘
                                     │
                           ┌─────────┴─────────┐
                           ▼                   ▼
                   ┌──────────────┐   ┌────────────────┐
                   │  SSE Stream  │   │  Frontend SPA  │
                   │  /api/alerts │   │  (static/ )    │
                   │  /stream     │   │  localhost:8080 │
                   └──────────────┘   └────────────────┘
```

### Execution modes

| Mode | Command | What runs | Use case |
|------|---------|-----------|----------|
| **All-in-one** | `make spring-boot` | Spring Boot + StreamsManager (7 topologies) + SSE + REST + frontend | Development, demo |
| **Standalone** | `make streams` | 7 topologies in a single JVM (no web server) | Testing streams only |
| **Separate** | Terminal 1: `make streams` + Terminal 2: `make spring-boot` | Streams + web backend in separate JVMs | Debugging, scaled |

---

## Technologies

| Technology | Version | Purpose |
|-----------|---------|---------|
| Java | 17 | Core language |
| Apache Kafka | 3.7.1 / 3.8.0 | Event streaming platform |
| Kafka Streams | 3.8.0 | Stream processing (CEP) — DSL only (no Processor API) |
| Spring Boot | 3.2.5 | Web backend (REST, SSE, static files) |
| Maven | — | Build & dependencies |
| Docker / Docker Compose | — | Local 3-broker Kafka cluster |
| Jackson | 2.17.0 | JSON serialization |
| Leaflet.js | 1.9.4 | Interactive fraud map |

---

## Prerequisites

- **Java 17+**
- **Maven 3.8+**
- **Docker** & **Docker Compose**

---

## Project Structure

```
.
├── compose.yaml                    # 3-broker Kafka cluster (KRaft, no ZooKeeper)
├── Makefile                        # Build, run, simulate commands
├── pom.xml                         # Maven config (Spring Boot, Kafka Streams, Jackson)
├── clients.json                    # 100 generated client profiles
├── kafka/
│   ├── broker1.properties          # Broker configs
│   ├── broker2.properties
│   └── broker3.properties
└── src/main/java/com/frauddetection/
    ├── config/
    │   └── KafkaConfig.java        # Topics, brokers, Streams/Producer/Consumer props
    ├── model/
    │   ├── TransactionEvent.java   # Bank transaction (PIX, CRED, DEB)
    │   ├── AuthEvent.java          # Login / password change with geolocation
    │   ├── FraudAlert.java         # Fraud alert with severity, type, geo
    │   └── GeoLocation.java        # Coordinates for Haversine distance
    ├── serialization/
    │   ├── JsonSerializer.java     # Generic JSON serializer
    │   ├── JsonDeserializer.java   # Generic JSON deserializer
    │   ├── JsonSerde.java          # Generic Serde for Kafka Streams
    │   └── JsonSerdes.java         # Serde factory for each model
    ├── sources/                    # Event producers (CLI)
    │   ├── LegitimateEventProducer.java
    │   ├── HighAmountFraudProducer.java
    │   ├── BurstTransactionFraudProducer.java
    │   ├── UnknownDeviceFraudProducer.java
    │   ├── PasswordChangeFraudProducer.java
    │   ├── AccountTakeoverFraudProducer.java
    │   ├── EmptyingAccountFraudProducer.java
    │   └── FarawayLoginFraudProducer.java
    ├── streams/                    # 7 independent Kafka Streams topologies
    │   ├── FraudDetectionTopology.java   # Factory: buildAll() returns List<KafkaStreams>
    │   ├── FraudDetectionApp.java        # Standalone entry point
    │   ├── HighAmountTopology.java       # Single tx > R$50,000
    │   ├── BurstTopology.java            # 5+ txs in 60s
    │   ├── UnknownDeviceTopology.java    # GlobalKTable join for untrusted devices
    │   ├── PasswordChangeTopology.java   # Password change event
    │   ├── AccountTakeoverTopology.java  # 3-step: login → pwChange → high tx
    │   ├── EmptyingAccountTopology.java  # 5+ high txs in 60s
    │   ├── FarawayLoginTopology.java     # KTable aggregate + flatMap + Haversine
    │   ├── StreamsManager.java           # @Component: starts/stop all topologies
    │   ├── AccountAggregate.java         # State class for EmptyingAccount
    │   ├── LoginPair.java                # State class for FarawayLogin
    │   └── TakeoverState.java            # State class for AccountTakeover
    ├── web/
    │   ├── FraudDetectionApplication.java    # Spring Boot main
    │   ├── EventController.java              # POST /api/events/{transaction,auth}
    │   └── AlertSSEController.java           # GET /api/alerts/stream (SSE)
    └── utils/
        ├── ClientGenerator.java    # Generates clients.json
        ├── ClientLoader.java       # Reads clients.json
        └── ClientProfile.java      # Client profile record

src/main/resources/static/          # Frontend SPA (served by Spring Boot)
├── index.html                      # SPA entry point
├── css/style.css                   # Dark theme
└── js/
    ├── app.js                      # Router, nav, stats
    ├── alerts.js                   # SSE + Leaflet map
    └── events.js                   # Fraud simulator + manual forms
```

---

## Quick Start

```bash
# 1. Start Kafka cluster (3 brokers)
make up

# 2. Create topics
make topics

# 3. Generate & publish client profiles
make clients

# 4. Build the project
make build

# 5. Start everything (Spring Boot + 7 topologies + frontend)
make spring-boot

# 6. Open http://localhost:8080

# 7. In another terminal, inject legitimate traffic
make simulate

# 8. In another terminal, test fraud scenarios
make faraway-login
```

---

## Setup & Execution

### 1. Start Kafka cluster

```bash
make up
```

Starts 3 KRaft brokers (no ZooKeeper):

| Broker | Port |
|--------|------|
| kafka-1 | `localhost:19092` |
| kafka-2 | `localhost:29092` |
| kafka-3 | `localhost:39092` |

### 2. Create topics

```bash
make topics
```

| Topic | Partitions | RF | Notes |
|-------|-----------|----|-------|
| `transactions.raw` | 3 | 3 | Transaction events |
| `auth.events` | 3 | 3 | Auth events (login, password_change) |
| `fraud.events` | 3 | 3 | Fraud alerts |
| `clients.profiles` | 1 | 3 | Compacted, for GlobalKTable |

### 3. Generate client profiles

```bash
make clients
```

Generates `clients.json` with 100 profiles and publishes each to `clients.profiles`.

### 4. Build

```bash
make build
```

Produces `target/kafka-finance-example-1.0-SNAPSHOT.jar`.

### 5. Run

**All-in-one (recommended):**
```bash
make spring-boot
```
Spring Boot starts on port 8080, serving the frontend SPA, REST API, SSE stream, and all 7 topologies via `StreamsManager`.

**Standalone streams only:**
```bash
make streams
```
Runs all 7 topologies without the web server. Useful for headless testing.

### 6. Observe alerts

```bash
# Terminal: listen to fraud events directly
make listen-alerts
```

Or open `http://localhost:8080` and navigate to **Live Alerts**.

---

## Make Targets

### Infrastructure

| Command | Description |
|---------|-------------|
| `make up` | Start Kafka cluster |
| `make down` | Stop & remove containers |
| `make restart` | Restart (recreate containers) |

### Build

| Command | Description |
|---------|-------------|
| `make build` | Compile with Maven |
| `make clean` | Remove build artifacts |

### Data

| Command | Description |
|---------|-------------|
| `make clients` | Generate `clients.json` + publish to `clients.profiles` |
| `make simulate` | Run LegitimateEventProducer (continuous) |

### Fraud Sources (CLI)

| Command | Fraud type |
|---------|------------|
| `make high-amount` | Single tx > R$50,000 |
| `make burst` | 10 rapid transactions |
| `make unknown-device` | Login from untrusted device |
| `make password-change` | Password change event |
| `make account-takeover` | Unknown login → pw change → high tx |
| `make emptying-account` | 4 high-value transactions |
| `make faraway-login` | Login SP → Tokyo in 500ms (requires previous login in state store) |

### Streams + Backend

| Command | Description |
|---------|-------------|
| `make spring-boot` | Spring Boot (port 8080) + 7 topologies + frontend |
| `make streams` | Standalone streams (7 topologies, no web) |
| `make listen-alerts` | Console consumer on `fraud.events` |

### Kafka Utils

| Command | Description |
|---------|-------------|
| `make topics` | Create topics |
| `make topics-view` | List topics |
| `make topics-describe` | Describe application topics |
| `make listen TOPIC=<name>` | Console consumer on any topic |

---

## Data Models

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
  "userId": "u-000",
  "accountId": "acc-u-000-0",
  "description": "Account takeover: unknown device login → password change → R$55000.00",
  "severity": "CRITICAL",
  "timestamp": 1715712000000,
  "latitude": -20.3,
  "longitude": -40.3
}
```

`latitude` / `longitude` are present only for alerts derived from `AuthEvent` (unknown device, account takeover, faraway login). `PASSWORD_CHANGE` alerts have fixed severity **LOW** and no geo coordinates (not shown on map). Alerts derived from transactions (high value, burst, emptying account) also have no geo coordinates.

---

## Topologies

Each topology runs as an **independent `KafkaStreams` instance** with its own `application.id`, consumer group, and state stores. This provides fault isolation — one topology crashing does not affect the others, and each can be scaled independently.

| # | Topology | `application.id` | Input | Logic | State |
|---|----------|------------------|-------|-------|-------|
| 1 | HighAmount | `fraud-detection-high-amount` | `transactions.raw` | Single tx > R$50,000 | Stateless |
| 2 | Burst | `fraud-detection-burst` | `transactions.raw` | 5+ txs in 60s window | Tumbling Window |
| 3 | UnknownDevice | `fraud-detection-unknown-device` | `transactions.raw` + `clients.profiles` | GlobalKTable join: device not in trusted list | GlobalKTable |
| 4 | PasswordChange | `fraud-detection-password-change` | `auth.events` | `password_change` event | Stateless |
| 5 | AccountTakeover | `fraud-detection-account-takeover` | `auth.events` + `transactions.raw` | KTable aggregate (login+pwChange) + KStream-KTable join (high tx) | KeyValueStore (`takeover-store`) |
| 6 | EmptyingAccount | `fraud-detection-emptying-account` | `transactions.raw` | 5+ txs ≥ R$1,000 in 60s | Tumbling Window |
| 7 | FarawayLogin | `fraud-detection-faraway-login` | `auth.events` | KTable aggregate (consecutive LoginPair) + toStream + flatMap: speed > 900 km/h | KeyValueStore (`last-login-store`) |

### 1. HighAmount

```
transactions.raw
  └── filter(amount > 50000)
        └── map → FraudAlert.highValue(tx)
              └── to fraud.events
```

**Threshold:** R$ 50,000 (hardcoded `LIMIT`). A single transaction above this limit triggers `HIGH_VALUE_TRANSACTION` (severity: HIGH).

### 2. Burst

```
transactions.raw
  └── groupByKey()
        └── windowedBy(TumblingWindow.of(Duration.ofSeconds(60)))
              └── count()
                    └── filter(count >= 5)
                          └── map → FraudAlert.transactionBurst(...)
                                └── to fraud.events
```

Triggers `TRANSACTION_BURST` (severity: MEDIUM) when 5+ transactions occur in a 60-second window for the same account.

### 3. UnknownDevice

```
clients.profiles → GlobalKTable(key=userId)
transactions.raw → KStream(key=userId)
  └── join(globalTable, ...)
        └── filter(deviceId NOT in trustedDevices)
              └── map → FraudAlert.unknownDevice(...)
                    └── to fraud.events
```

Joins transactions against the full in-memory client table. If the transaction's `deviceId` is not in the client's `trustedDevices`, triggers `UNKNOWN_DEVICE` (severity: MEDIUM).

### 4. PasswordChange

```
auth.events
  └── filter(eventType == "password_change")
        └── map → FraudAlert.passwordChange(auth)
              └── to fraud.events
```

Triggers `PASSWORD_CHANGE` (severity: LOW) on any `password_change` auth event. Not shown on the Leaflet map (no geo coordinates).

### 5. AccountTakeover

```
auth.events
  └── groupByKey()
        └── aggregate(TakeoverState, ...) → KTable<String, TakeoverState>
              login → loginSeen=true
              password_change → pwChangeSeen=true

transactions.raw
  └── filter(amount >= 5000) → KStream<String, TransactionEvent>
        └── join(authState KTable) → (tx, state)
              ├── loginSeen && pwChangeSeen → FraudAlert.accountTakeover(tx)
              └── otherwise → null
                    └── filter(null) → fraud.events
```

Uses a `KTable` aggregate (`groupByKey().aggregate()`) on `auth.events` to track which users have completed both steps (login + password change). Then joins this KTable with high-value transactions (`KStream-KTable` join). When a user has both flags set and performs a transaction of R$5,000+, triggers `ACCOUNT_TAKEOVER` (severity: CRITICAL).

- State store: `takeover-store` (RocksDB, persistent)

### 6. EmptyingAccount

```
transactions.raw
  └── filter(amount >= 1000)
        └── groupByKey()
              └── windowedBy(TumblingWindow.of(Duration.ofSeconds(60)))
                    └── aggregate(AccountAggregate, ...)
                          └── filter(count >= 5)
                                └── map → FraudAlert.emptyingAccount(...)
                                      └── to fraud.events
```

Triggers `EMPTYING_ACCOUNT` (severity: HIGH) when 5+ transactions of at least R$1,000 occur in 60 seconds for the same account.

### 7. FarawayLogin

```
auth.events
  └── filter(eventType == "login")
        └── groupByKey()
              └── aggregate(LoginPair, ...) → KTable<String, LoginPair>
                    pair.previous = (old) pair.current
                    pair.current = newAuth
                      └── toStream()
                            └── flatMap → if previous != null && speed > 900 km/h
                                  └── FraudAlert.farawayLogin(curr, distance, speed)
                                        └── to fraud.events
```

Uses a `KTable` aggregate (`groupByKey().aggregate()`) to track consecutive logins per user as a `LoginPair` (previous, current). The `toStream().flatMap()` computes the Haversine distance and travel speed between consecutive logins. If speed exceeds 900 km/h, triggers `FARAWAY_LOGIN` (severity: HIGH).

- State store: `last-login-store` (RocksDB, persistent)
- Example: SP to Tokyo in 500ms ≈ 133,000,000 km/h → alert.

---

## REST API

All endpoints are served on `http://localhost:8080`.

### Events

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/events/transaction` | Publish a `TransactionEvent` |
| `POST` | `/api/events/auth` | Publish an `AuthEvent` |

### SSE Stream

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/alerts/stream` | SSE — real-time fraud alerts (event name: `alert`) |

---

## Frontend

A pure HTML + JS SPA (no framework), served by Spring Boot from `src/main/resources/static/`.

### Pages

| Page | Route | Description |
|------|-------|-------------|
| Dashboard | `#dashboard` | Stats cards (alerts by severity) + recent alerts feed |
| Live Alerts | `#alerts` | SSE alert feed + Leaflet map with geo markers |
| Simulator | `#simulator` | One-click fraud scenario buttons |
| Create Event | `#create` | Transaction form + simulated Login / Change Password UI |

### Live Alerts Map

- Leaflet.js map centred on Brazil (zoom 4)
- Circle markers coloured by severity:
  - 🔴 CRITICAL → red
  - 🟡 HIGH → amber
  - 🔵 MEDIUM → blue
  - 🟢 LOW → green
- Click for popup: alert type, description, timestamp
- Clear button resets feed + markers
- Auto-reconnects on SSE disconnect

### Simulator

One-click buttons for each fraud scenario:
- **High Amount** — single tx > R$50,000
- **Burst** — 5 rapid txs from the same user (fixed: previously used different users)
- **Unknown Device** — auth from unknown device
- **Password Change** — password change event
- **Account Takeover** — login (unknown device) → password change → high-value tx
- **Emptying Account** — 4 high-value txs from the same user
- **Faraway Login** — 2 logins (SP + random destination) to trigger impossible travel

### Create Event: Login

Simulated login form (not a real login — it publishes an `AuthEvent`):

- **Username** → `userId`
- **Password** — visual only, not sent
- **Remember this device** checkbox:
  - Checked → trusted device (no alert)
  - Unchecked → unknown device (triggers `UNKNOWN_DEVICE` alert)

### Create Event: Change Password

Simulated password change form:

- **Username** → `userId`
- Passwords must match (client-side validation)
- Publishes a `password_change` `AuthEvent` (triggers `PASSWORD_CHANGE` alert)

All technical fields (`eventId`, `deviceId`, `ipAddress`, `latitude`, `longitude`) are auto-generated on submit.

---

## CLI Sources

### LegitimateEventProducer

Generates normal traffic continuously:

- 70% chance of a transaction (R$150–200), 30% auth event
- Uses only trusted devices and home IPs
- Publishes to `transactions.raw` and `auth.events`

### Fraud Sources

Each CLI source injects a specific fraud pattern:

| Source | Behaviour |
|--------|-----------|
| `HighAmountFraudProducer` | Single transaction R$80,000–150,000 |
| `BurstTransactionFraudProducer` | 10 rapid transactions |
| `UnknownDeviceFraudProducer` | Auth with random deviceId |
| `PasswordChangeFraudProducer` | Login from unknown device → password change |
| `AccountTakeoverFraudProducer` | Unknown login → pw change → high-value tx |
| `EmptyingAccountFraudProducer` | 4 transactions of R$3,000–5,000 |
| `FarawayLoginFraudProducer` | Login SP → Tokyo in 500ms |

---

## License

This project is licensed under [Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)](http://creativecommons.org/licenses/by-sa/4.0/).
