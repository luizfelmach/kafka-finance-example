# Makefile - Fraud Detection
# Utility commands to manage the fraud detection project with Kafka

COMPOSE_FILE := compose.yaml
JAVA_JAR := target/kafka-finance-example-1.0-SNAPSHOT.jar
CLIENTS_FILE := clients.json
SCALE ?= 1

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo ""
	@echo "Fraud Detection"
	@echo "Usage: make <target> [VAR=value]"
	@echo ""
	@echo "Compose"
	@printf "  \033[36m%-14s\033[0m %s\n" "up" "Start all services with Docker Compose (builds image if needed)"
	@printf "  \033[36m%-14s\033[0m %s\n" "down" "Stop and remove all Docker Compose services"
	@printf "  \033[36m%-14s\033[0m %s\n" "restart" "Restart all services (recreates containers to pick up new env vars)"
	@echo ""
	@echo "Java"
	@printf "  \033[36m%-14s\033[0m %s\n" "build" "Build project JAR with Maven"
	@printf "  \033[36m%-14s\033[0m %s\n" "clean" "Remove local Maven build artifacts"
	@printf "  \033[36m%-14s\033[0m %s\n" "clients" "Generate simulated client profiles"
	@printf "  \033[36m%-14s\033[0m %s\n" "simulate" "Simulate legitimate transactions and auth events"
	@echo ""
	@echo "Fraud Sources (CLI)"
	@printf "  \033[36m%-14s\033[0m %s\n" "high-amount" "Simulate high amount fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "burst" "Simulate burst transaction fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "unknown-device" "Simulate unknown device fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "password-change" "Simulate password change fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "account-takeover" "Simulate account takeover fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "emptying-account" "Simulate emptying account fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "parallel-login" "Simulate parallel login fraud (SP + Recife)"
	@printf "  \033[36m%-14s\033[0m %s\n" "faraway-login" "Simulate faraway login fraud (SP + Tokyo)"
	@printf "  \033[36m%-14s\033[0m %s\n" "under-observation" "Simulate under observation detection"
	@echo ""
	@echo "Spring Boot (Backend + Streams)"
	@printf "  \033[36m%-14s\033[0m %s\n" "spring-boot" "Start Spring Boot backend (port 8080) with all 9 topologies"
	@echo ""
	@echo "Kafka Streams (Standalone)"
	@printf "  \033[36m%-14s\033[0m %s\n" "streams" "Start Kafka Streams FraudDetectionApp (standalone)"
	@printf "  \033[36m%-14s\033[0m %s\n" "listen-alerts" "Listen to fraud.events topic for alerts"
	@echo ""
	@echo "Kafka"
	@printf "  \033[36m%-14s\033[0m %s\n" "topics" "Create the 4 required Kafka topics (3 partitions, RF=3)"
	@printf "  \033[36m%-14s\033[0m %s\n" "topics-view" "List all Kafka topics"
	@printf "  \033[36m%-14s\033[0m %s\n" "topics-describe" "Show detailed info for all application topics"
	@printf "  \033[36m%-14s\033[0m %s\n" "listen" "Listen to a topic (TOPIC=name make listen)"
	@echo ""

up: ## Start all services with Docker Compose (builds images if needed)
	@echo "→ Starting services with Docker Compose..."
	docker compose -f $(COMPOSE_FILE) up -d --build
	@echo "✓ Services started"

down: ## Stop and remove all Docker Compose services
	@echo "→ Stopping Docker Compose services..."
	docker compose -f $(COMPOSE_FILE) down
	@echo "✓ Services stopped"

restart: ## Restart all services (recreates containers to pick up new env vars)
	@echo "→ Restarting services (recreating containers)..."
	docker compose -f $(COMPOSE_FILE) up -d --force-recreate
	@echo "✓ Services restarted"

build: clean ## Build project JAR with Maven
	@echo "→ Building project JAR..."
	mvn -q -DskipTests package
	@echo "✓ Build complete"

clean: ## Clean Maven build artifacts
	@echo "→ Cleaning build artifacts..."
	@mvn clean -q
	@echo "✓ Clean complete"

clients: build ## Generate clients.json with simulated client metadata
	@echo "→ Generating clients metadata..."
	java -cp $(JAVA_JAR) com.frauddetection.utils.ClientGenerator 100 $(CLIENTS_FILE)
	@echo "✓ Clients generated -> $(CLIENTS_FILE)"

simulate: build ## Run LegitimateEventProducer
	@echo "→ Running LegitimateEventProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.LegitimateEventProducer

streams: build ## Start Kafka Streams FraudDetectionApp (all 9 topologies, standalone)
	@echo "→ Starting FraudDetectionApp (Kafka Streams)..."
	java -cp $(JAVA_JAR) com.frauddetection.streams.FraudDetectionApp

listen-alerts: ## Listen to fraud.events topic for alerts
	@echo "→ Listening to fraud.events..."
	docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --topic fraud.events --bootstrap-server localhost:9092 --from-beginning

spring-boot: build ## Start Spring Boot backend (port 8080) with embedded Streams
	@echo "→ Starting Spring Boot application..."
	java -cp $(JAVA_JAR) com.frauddetection.web.FraudDetectionApplication

topics: ## Create the 4 required Kafka topics (transactions.raw, auth.events, fraud.events 3p/RF=3, clients.profiles 1p/compact)
	@echo "→ Creating Kafka topics..."
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions.raw --partitions 3 --replication-factor 3
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic auth.events --partitions 3 --replication-factor 3
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fraud.events --partitions 3 --replication-factor 3
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic clients.profiles --partitions 1 --replication-factor 3 --config cleanup.policy=compact
	@echo "✓ Topics created"

topics-view: ## List all Kafka topics
	@echo "→ Listing Kafka topics..."
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

topics-describe: ## Show detailed info for all application topics
	@echo "→ Describing application topics..."
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions.raw
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic auth.events
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic fraud.events
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic clients.profiles

listen: ## Listen to a Kafka topic (usage: TOPIC=name make listen)
	@if [ -z "$(TOPIC)" ]; then echo "Error: TOPIC is not set. Usage: TOPIC=name make listen"; exit 1; fi
	@echo "→ Listening to topic '$(TOPIC)'..."
	docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --topic $(TOPIC) --bootstrap-server localhost:9092

high-amount: build ## Simulate high amount fraud
	@echo "→ Running HighAmountFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.HighAmountFraudProducer

burst: build ## Simulate burst transaction fraud
	@echo "→ Running BurstTransactionFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.BurstTransactionFraudProducer

unknown-device: build ## Simulate unknown device fraud
	@echo "→ Running UnknownDeviceFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.UnknownDeviceFraudProducer

password-change: build ## Simulate password change fraud
	@echo "→ Running PasswordChangeFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.PasswordChangeFraudProducer

account-takeover: build ## Simulate account takeover fraud (combines multiple fraud types)
	@echo "→ Running AccountTakeoverFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.AccountTakeoverFraudProducer

emptying-account: build ## Simulate emptying account fraud (combine multiple fraud types)
	@echo "→ Running EmptyingAccountFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.EmptyingAccountFraudProducer

parallel-login: build ## Simulate parallel login fraud (SP + Recife)
	@echo "→ Running ParallelLoginFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.ParallelLoginFraudProducer

faraway-login: build ## Simulate faraway login fraud (SP + Tokyo)
	@echo "→ Running FarawayLoginFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.FarawayLoginFraudProducer

under-observation: build ## Simulate under observation detection
	@echo "→ Running UnderObservationFraudSource..."
	java -cp $(JAVA_JAR) com.frauddetection.sources.UnderObservationFraudProducer

.PHONY: help up down restart build clean clients simulate listen topics topics-view topics-describe high-amount burst unknown-device password-change account-takeover emptying-account parallel-login faraway-login under-observation streams listen-alerts spring-boot
