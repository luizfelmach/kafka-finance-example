# Makefile - Fraud Detection
# Utility commands to manage the fraud detection project with Kafka

COMPOSE_FILE := compose.yaml
JAVA_JAR := target/kafka-finance-example-1.0-SNAPSHOT.jar
CLIENTS_FILE := clients.json

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
	@echo "Fraud Producers"
	@printf "  \033[36m%-14s\033[0m %s\n" "high-amount" "Simulate high amount fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "burst" "Simulate burst transaction fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "unknown-device" "Simulate unknown device fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "password-change" "Simulate password change fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "account-takeover" "Simulate account takeover fraud"
	@echo ""
	@echo "Fraud Consumers"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-high-amount" "Detect high amount fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-burst" "Detect burst transaction fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-unknown-device" "Detect unknown device fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-password-change" "Detect password change fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-account-takeover" "Detect account takeover fraud"
	@echo ""
	@echo "Tmux"
	@printf "  \033[36m%-14s\033[0m %s\n" "tmux" "Open 5 tmux panes with all fraud detectors"
	@printf "  \033[36m%-14s\033[0m %s\n" "tmux-kill" "Kill the tmux session"
	@echo ""
	@echo "Kafka"
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

build: ## Build project JAR with Maven
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
	java -cp $(JAVA_JAR) com.frauddetection.producers.LegitimateEventProducer

listen: ## Listen to a Kafka topic (usage: TOPIC=name make listen)
	@if [ -z "$(TOPIC)" ]; then echo "Error: TOPIC is not set. Usage: TOPIC=name make listen"; exit 1; fi
	@echo "→ Listening to topic '$(TOPIC)'..."
	docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --topic $(TOPIC) --bootstrap-server localhost:9092

high-amount: build ## Simulate high amount fraud
	@echo "→ Running HighAmountFraudProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.producers.HighAmountFraudProducer

burst: build ## Simulate burst transaction fraud
	@echo "→ Running BurstTransactionFraudProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.producers.BurstTransactionFraudProducer

unknown-device: build ## Simulate unknown device fraud
	@echo "→ Running UnknownDeviceFraudProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.producers.UnknownDeviceFraudProducer

password-change: build ## Simulate password change fraud
	@echo "→ Running PasswordChangeFraudProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.producers.PasswordChangeFraudProducer

account-takeover: build ## Simulate account takeover fraud (combines multiple fraud types)
	@echo "→ Running AccountTakeoverFraudProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.producers.AccountTakeoverFraudProducer

emptying-account: build ## Simulate emptying account fraud (combine multiple fraud types)
	@echo "→ Running EmptyingAccountFraudProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.producers.EmptyingAccountFraudProducer

detect-high-amount: build ## Detect high amount fraud
	@echo "→ Running HighAmountConsumer..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.HighAmountConsumer

detect-burst: build ## Detect burst transaction fraud
	@echo "→ Running BurstTransactionConsumer..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.BurstTransactionConsumer

detect-unknown-device: build ## Detect unknown device fraud
	@echo "→ Running UnknownDeviceConsumer..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.UnknownDeviceConsumer

detect-password-change: build ## Detect password change fraud
	@echo "→ Running PasswordChangeConsumer..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.PasswordChangeConsumer

detect-account-takeover: build ## Detect account takeover fraud
	@echo "→ Running AccountTakeoverConsumerProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.AccountTakeoverConsumerProducer

detect-emptying-account: build ## Detect emptying account fraud
	@echo "→ Running EmptyingAccountConsumerProducer..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.EmptyingAccountConsumerProducer

tmux: build ## Open 5 tmux panes with all fraud detectors
	@echo "→ Starting tmux session with fraud detectors..."
	tmux new-session -d -s fraud-detection -n detectors
	tmux split-window -t fraud-detection
	tmux select-layout -t fraud-detection tiled
	tmux split-window -t fraud-detection
	tmux select-layout -t fraud-detection tiled
	tmux split-window -t fraud-detection
	tmux select-layout -t fraud-detection tiled
	tmux split-window -t fraud-detection
	tmux select-layout -t fraud-detection tiled
	tmux split-window -t fraud-detection
	tmux select-layout -t fraud-detection tiled
	tmux send-keys -t fraud-detection:0.0 'java -cp $(JAVA_JAR) com.frauddetection.consumers.HighAmountConsumer' C-m
	tmux send-keys -t fraud-detection:0.1 'java -cp $(JAVA_JAR) com.frauddetection.consumers.BurstTransactionConsumer' C-m
	tmux send-keys -t fraud-detection:0.2 'java -cp $(JAVA_JAR) com.frauddetection.consumers.UnknownDeviceConsumer' C-m
	tmux send-keys -t fraud-detection:0.3 'java -cp $(JAVA_JAR) com.frauddetection.consumers.PasswordChangeConsumer' C-m
	tmux send-keys -t fraud-detection:0.4 'java -cp $(JAVA_JAR) com.frauddetection.consumers.AccountTakeoverConsumerProducer' C-m
	tmux send-keys -t fraud-detection:0.5 'java -cp $(JAVA_JAR) com.frauddetection.consumers.EmptyingAccountConsumerProducer' C-m
	tmux attach -t fraud-detection

tmux-kill: ## Kill the tmux session
	@echo "→ Killing tmux session..."
	tmux kill-session -t fraud-detection

.PHONY: help up down restart build clean clients simulate listen high-amount burst unknown-device password-change account-takeover detect-high-amount detect-burst detect-unknown-device detect-password-change detect-account-takeover detect-emptying-account tmux
