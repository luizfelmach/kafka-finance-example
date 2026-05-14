# Kafka Finance Example — Sistema de Detecção de Fraudes

Sistema de detecção de fraudes financeiras em tempo real utilizando **Apache Kafka** como plataforma de streaming de eventos. O projeto simula transações bancárias, eventos de autenticação e detecta padrões suspeitos através de consumidores especializados.

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
- [Consumidores e Detectores](#consumidores-e-detectores)
- [Produtores de Eventos](#produtores-de-eventos)
- [Licença](#licença)

---

## Visão Geral

Este projeto demonstra um pipeline completo de detecção de fraudes financeiras:

1. **Geração de clientes simulados** — perfis com contas, dispositivos confiáveis e IPs.
2. **Produção de eventos legítimos** — transações e autenticações normais.
3. **Simulação de fraudes** — produtores especializados injetam comportamentos maliciosos.
4. **Detecção em tempo real** — consumidores Kafka analisam padrões e geram alertas.

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        PRODUTORES                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ LegitimateEvent │  │ FraudProducers  │  │ FraudProducers  │ │
│  │   Producer      │  │  (6 tipos)      │  │  (6 tipos)      │ │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘ │
└───────────┼────────────────────┼────────────────────┼──────────┘
            │                    │                    │
            ▼                    ▼                    ▼
   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
   │ transactions   │   │   auth.events  │   │  fraud.events  │
   │    .raw        │   │                │   │                │
   │  (3 partições) │   │  (3 partições) │   │  (3 partições) │
   │    RF = 3      │   │    RF = 3      │   │    RF = 3      │
   └───────┬────────┘   └───────┬────────┘   └────────────────┘
           │                    │
           ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CONSUMIDORES / DETECTORES                   │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────┐ │
│  │ HighAmount  │ │   Burst     │ │   Unknown   │ │ Password   │ │
│  │  Consumer   │ │ Transaction │ │   Device    │ │  Change    │ │
│  │             │ │  Consumer   │ │  Consumer   │ │  Consumer  │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └────────────┘ │
│  ┌─────────────────┐ ┌─────────────────┐                         │
│  │ AccountTakeover │ │ EmptyingAccount │                         │
│  │ConsumerProducer │ │ConsumerProducer │                         │
│  └─────────────────┘ └─────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tecnologias

| Tecnologia | Versão | Descrição |
|------------|--------|-----------|
| Java | 17 | Linguagem principal |
| Apache Kafka | 3.7.1 / 3.8.0 | Plataforma de streaming |
| Maven | — | Build e dependências |
| Docker / Docker Compose | — | Cluster Kafka local |
| Jackson | 2.17.0 | Serialização JSON |
| SLF4J | 2.0.13 | Logging |
| Tmux | — | Orquestração de terminais |

---

## Pré-requisitos

- **Java 17** ou superior
- **Maven 3.8+**
- **Docker** e **Docker Compose**
- **Tmux** (opcional, para execução paralela dos detectores)

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
    │   └── KafkaConfig.java        # Configuração de brokers e tópicos
    ├── model/
    │   ├── TransactionEvent.java   # Evento de transação
    │   ├── AuthEvent.java          # Evento de autenticação
    │   └── FraudAlert.java         # Alerta de fraude
    ├── serialization/
    │   ├── JsonSerializer.java
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
    │   └── EmptyingAccountFraudProducer.java
    ├── consumers/
    │   ├── HighAmountConsumer.java
    │   ├── BurstTransactionConsumer.java
    │   ├── UnknownDeviceConsumer.java
    │   ├── PasswordChangeConsumer.java
    │   ├── AccountTakeoverConsumerProducer.java
    │   └── EmptyingAccountConsumerProducer.java
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

Cria os 3 tópicos com 3 partições e fator de replicação 3:
- `transactions.raw`
- `auth.events`
- `fraud.events`

### 3. Gerar clientes simulados

```bash
make clients
```

Gera o arquivo `clients.json` com 100 perfis de clientes contendo:
- `user_id`
- `accounts` (1–2 contas)
- `trusted_devices` (1–2 dispositivos)
- `home_ip`

### 4. Compilar o projeto

```bash
make build
```

Gera o JAR sombreado em `target/kafka-finance-example-1.0-SNAPSHOT.jar`.

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
| `make clients` | Gera `clients.json` com 100 clientes |
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

### Consumidores / Detectores

| Comando | Detector |
|---------|----------|
| `make detect-high-amount` | Detecção de valor alto |
| `make detect-burst` | Detecção de rajada |
| `make detect-unknown-device` | Detecção de dispositivo desconhecido |
| `make detect-password-change` | Detecção de troca de senha |
| `make detect-account-takeover` | Detecção de takeover de conta |
| `make detect-emptying-account` | Detecção de esvaziamento de conta |

### Tmux (execução paralela)

| Comando | Descrição |
|---------|-----------|
| `make tmux` | Abre 6 painéis com todos os detectores simultaneamente |
| `make tmux-kill` | Encerra a sessão tmux |

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
| **HIGH_AMOUNT** | Transação com valor muito acima da média histórica | Se primeira transação > R$ 8.000 OU valor > 3× média móvel (janela de 5 min) |
| **BURST_TRANSACTIONS** | Muitas transações em curto intervalo | ≥ 5 transações em 60 segundos por conta |
| **UNKNOWN_DEVICE** | Dispositivo não cadastrado realiza transação | Verifica `deviceId` contra `trusted_devices` do perfil |
| **PASSWORD_CHANGE** | Alteração de senha seguida de transação suspeita | Troca de senha após login de dispositivo desconhecido |
| **ACCOUNT_TAKEOVER** | Sequência completa de invasão | Login desconhecido → troca de senha → transação de alto valor (dentro de 10 min) |
| **EMPTYING_ACCOUNT** | Tentativa de esvaziar saldo da conta | Múltiplas transações de alto valor em sequência |

---

## Tópicos Kafka

| Tópico | Partições | Replicação | Conteúdo |
|--------|-----------|------------|----------|
| `transactions.raw` | 3 | 3 | Eventos de transação (PIX, CRED, DEB) |
| `auth.events` | 3 | 3 | Eventos de autenticação (login, password_change) |
| `fraud.events` | 3 | 3 | Alertas de fraude gerados pelos consumidores |

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
  "destinationAccount": "acc-u-001-0"
}
```

### AuthEvent

```json
{
  "eventId": "auth-e1f2g3h4",
  "userId": "u-000",
  "eventType": "login",
  "deviceId": "dev-u-000-0",
  "ipAddress": "177.10.174.12"
}
```

### FraudAlert

```json
{
  "alertType": "ACCOUNT_TAKEOVER",
  "userId": "u-000",
  "description": "Account takeover: Suspect login -> Password change -> High value transaction (R$5000.00)",
  "timestamp": 1715712000000
}
```

---

## Consumidores e Detectores

### HighAmountConsumer
- **Entrada:** `transactions.raw`
- **Lógica:** Mantém histórico de valores por conta (janela de 5 min). Alerta se valor > 3× média ou primeira transação > R$ 8.000.

### BurstTransactionConsumer
- **Entrada:** `transactions.raw`
- **Lógica:** Conta transações por conta em janela de 60 segundos. Alerta se ≥ 5 transações.

### UnknownDeviceConsumer
- **Entrada:** `transactions.raw`
- **Lógica:** Compara `deviceId` da transação contra dispositivos confiáveis do `clients.json`.

### PasswordChangeConsumer
- **Entrada:** `auth.events`
- **Lógica:** Detecta troca de senha (`password_change`) precedida por login de dispositivo desconhecido.

### AccountTakeoverConsumerProducer
- **Entradas:** `auth.events` + `transactions.raw`
- **Lógica:** State machine que rastreia: login desconhecido → troca de senha → transação de alto valor. Emite alerta para `fraud.events`.

### EmptyingAccountConsumerProducer
- **Entrada:** `transactions.raw`
- **Lógica:** Detecta sequência de transações de alto valor que indicam tentativa de esvaziar a conta. Emite alerta para `fraud.events`.

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

---

## Exemplo de Fluxo Completo

```bash
# Terminal 1 — infraestrutura
make up
make topics
make clients
make build

# Terminal 2 — detectores (todos de uma vez)
make tmux

# Terminal 3 — eventos legítimos
make simulate

# Terminal 4 — injetar fraude
make high-amount
make burst
make unknown-device
```
