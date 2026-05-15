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
	@echo "Fraud Producers"
	@printf "  \033[36m%-14s\033[0m %s\n" "high-amount" "Simulate high amount fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "burst" "Simulate burst transaction fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "unknown-device" "Simulate unknown device fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "password-change" "Simulate password change fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "account-takeover" "Simulate account takeover fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "emptying-account" "Simulate emptying account fraud"
	@echo ""
	@echo "Fraud Consumers"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-high-amount" "Detect high amount fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-burst" "Detect burst transaction fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-unknown-device" "Detect unknown device fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-password-change" "Detect password change fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-account-takeover" "Detect account takeover fraud"
	@printf "  \033[36m%-14s\033[0m %s\n" "detect-emptying-account" "Detect emptying account fraud"
	@echo ""
	@echo "Tmux"
	@printf "  \033[36m%-14s\033[0m %s\n" "tmux" "Open 6 tmux panes with all fraud detectors"
	@printf "  \033[36m%-14s\033[0m %s\n" "tmux-scaled" "Launch N consumer instances in tmux (CONSUMER=high-amount|burst|unknown-device|password-change|account-takeover|emptying-account, SCALE=N)"
	@printf "  \033[36m%-14s\033[0m %s\n" "tmux-kill" "Kill the tmux session"
	@echo ""
	@echo "Kafka"
	@printf "  \033[36m%-14s\033[0m %s\n" "topics" "Create the 3 required Kafka topics (3 partitions, RF=3)"
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

topics: ## Create the 3 required Kafka topics (3 partitions, RF=3)
	@echo "→ Creating Kafka topics..."
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions.raw --partitions 3 --replication-factor 3
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic auth.events --partitions 3 --replication-factor 3
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic fraud.events --partitions 3 --replication-factor 3
	@echo "✓ Topics created"

topics-view: ## List all Kafka topics
	@echo "→ Listing Kafka topics..."
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

topics-describe: ## Show detailed info for all application topics
	@echo "→ Describing application topics..."
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions.raw
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic auth.events
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic fraud.events

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

tmux-scaled: CONSUMER ?= high-amount
tmux-scaled: SCALE ?= 3
tmux-scaled: build ## Launch N consumer instances in tmux (CONSUMER=name, SCALE=N)
	@case "$(CONSUMER)" in \
		high-amount)      C="HighAmountConsumer";              G="tmux-high-amount" ;; \
		burst)            C="BurstTransactionConsumer";        G="tmux-burst" ;; \
		unknown-device)   C="UnknownDeviceConsumer";           G="tmux-unknown-device" ;; \
		password-change)  C="PasswordChangeConsumer";          G="tmux-password-change" ;; \
		account-takeover) C="AccountTakeoverConsumerProducer"; G="tmux-account-takeover" ;; \
		emptying-account) C="EmptyingAccountConsumerProducer"; G="tmux-emptying-account" ;; \
		*) echo "Unknown consumer '$(CONSUMER)'. Options: high-amount, burst, unknown-device, password-change, account-takeover, emptying-account"; exit 1 ;; \
	esac; \
	echo "→ Starting tmux: $(SCALE)x $$C (group=$$G)..."; \
	tmux new-session -d -s fraud-scaled -n "$$C"; \
	for i in $$(seq 2 $(SCALE)); do \
		tmux split-window -t fraud-scaled; \
		tmux select-layout -t fraud-scaled tiled; \
	done; \
	for i in $$(seq 0 $$(( $(SCALE) - 1 ))); do \
		tmux send-keys -t fraud-scaled:0.$$i "java -cp $(JAVA_JAR) com.frauddetection.consumers.$$C \"$$G\"" C-m; \
	done; \
	tmux attach -t fraud-scaled

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

tmux-kill: ## Kill all tmux sessions (fraud-detection, fraud-scaled)
	@echo "→ Killing tmux sessions..."
	-tmux kill-session -t fraud-detection 2>/dev/null
	-tmux kill-session -t fraud-scaled 2>/dev/null

.PHONY: help up down restart build clean clients simulate listen topics topics-view topics-describe high-amount burst unknown-device password-change account-takeover emptying-account detect-high-amount detect-burst detect-unknown-device detect-password-change detect-account-takeover detect-emptying-account tmux tmux-scaled tmux-kill
