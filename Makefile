# Makefile - Fraud Detection
# Utility commands to manage the fraud detection project with Kafka

COMPOSE_FILE := compose.yaml
JAR_FILE := target/kafka-finance-example-1.0-SNAPSHOT.jar

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
	@printf "  \033[36m%-14s\033[0m %s\n" "build" "Compile the Java project with Maven"
	@printf "  \033[36m%-14s\033[0m %s\n" "clean" "Clean Maven build artifacts"
	@printf "  \033[36m%-14s\033[0m %s\n" "producer1" "Run Producer1"
	@printf "  \033[36m%-14s\033[0m %s\n" "producer2" "Run Producer2"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer1" "Run Consumer1"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer2" "Run Consumer2"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer3" "Run Consumer3"
	@echo ""
	@echo "Examples:"
	@echo "  make up"
	@echo "  make down"
	@echo "  make restart"
	@echo "  make build"
	@echo "  make producer1"
	@echo ""

up: ## Start all services with Docker Compose (builds image if needed)
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

build: ## Compile the Java project with Maven
	@echo "→ Building Java project..."
	@mvn package -DskipTests -q
	@echo "✓ Build complete"

clean: ## Clean Maven build artifacts
	@echo "→ Cleaning build artifacts..."
	@mvn clean -q
	@echo "✓ Clean complete"

producer1: build ## Run Producer1
	@echo "→ Running Producer1..."
	@java -cp $(JAR_FILE) com.frauddetection.producers.Producer1

producer2: build ## Run Producer2
	@echo "→ Running Producer2..."
	@java -cp $(JAR_FILE) com.frauddetection.producers.Producer2

consumer1: build ## Run Consumer1
	@echo "→ Running Consumer1..."
	@java -cp $(JAR_FILE) com.frauddetection.consumers.Consumer1

consumer2: build ## Run Consumer2
	@echo "→ Running Consumer2..."
	@java -cp $(JAR_FILE) com.frauddetection.consumers.Consumer2

consumer3: build ## Run Consumer3
	@echo "→ Running Consumer3..."
	@java -cp $(JAR_FILE) com.frauddetection.consumers.Consumer3

.PHONY: help up down restart build clean producer1 producer2 consumer1 consumer2 consumer3
