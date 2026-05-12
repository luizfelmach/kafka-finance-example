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
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer1" "Run Consumer1"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer2" "Run Consumer2"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer3" "Run Consumer3"
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

build: ## Build project JAR on host with Maven
	@echo "→ Building project JAR on host..."
	mvn -q -DskipTests package
	@echo "✓ Build complete"

clean: ## Clean Maven build artifacts
	@echo "→ Cleaning build artifacts..."
	@mvn clean -q
	@echo "✓ Clean complete"

clients: build ## Generate clients.json with simulated client metadata
	@echo "→ Generating clients metadata on host..."
	java -cp $(JAVA_JAR) com.frauddetection.utils.ClientGenerator 100 $(CLIENTS_FILE)
	@echo "✓ Clients generated -> $(CLIENTS_FILE)"

simulate: build ## Run LegitimateEventProducer on host
	@echo "→ Running LegitimateEventProducer on host..."
	java -cp $(JAVA_JAR) com.frauddetection.producers.LegitimateEventProducer

listen: ## Listen to a Kafka topic (usage: TOPIC=name make listen)
	@if [ -z "$(TOPIC)" ]; then echo "Error: TOPIC is not set. Usage: TOPIC=name make listen"; exit 1; fi
	@echo "→ Listening to topic '$(TOPIC)'..."
	docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --topic $(TOPIC) --bootstrap-server localhost:9092

consumer1: build ## Run Consumer1 on host
	@echo "→ Running Consumer1 on host..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.Consumer1

consumer2: build ## Run Consumer2 on host
	@echo "→ Running Consumer2 on host..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.Consumer2

consumer3: build ## Run Consumer3 on host
	@echo "→ Running Consumer3 on host..."
	java -cp $(JAVA_JAR) com.frauddetection.consumers.Consumer3

.PHONY: help up down restart build clean clients simulate listen consumer1 consumer2 consumer3
