# Makefile - Fraud Detection
# Utility commands to manage the fraud detection project with Kafka

COMPOSE_FILE := compose.yaml
JAVA_CONTAINER := java-runner
JAVA_JAR := /app/kafka-finance-example.jar
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
	@printf "  \033[36m%-14s\033[0m %s\n" "build" "Build java-runner image (compiles Java classes)"
	@printf "  \033[36m%-14s\033[0m %s\n" "clean" "Remove local Maven build artifacts"
	@printf "  \033[36m%-14s\033[0m %s\n" "clients" "Generate clients.json via docker exec"
	@printf "  \033[36m%-14s\033[0m %s\n" "producer1" "Run Producer1 via docker exec"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer1" "Run Consumer1 via docker exec"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer2" "Run Consumer2 via docker exec"
	@printf "  \033[36m%-14s\033[0m %s\n" "consumer3" "Run Consumer3 via docker exec"
	@printf "  \033[36m%-14s\033[0m %s\n" "run" "Run CLASS=<java.class> with optional ARGS"
	@echo ""
	@echo "Examples:"
	@echo "  make up"
	@echo "  make down"
	@echo "  make restart"
	@echo "  make build"
	@echo "  make clients"
	@echo "  make producer1"
	@echo "  make run CLASS=com.frauddetection.consumers.Consumer1"
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

build: ## Build java-runner image (compiles Java classes)
	@echo "→ Building java-runner image..."
	docker compose -f $(COMPOSE_FILE) build java-runner
	@echo "✓ Build complete"

clean: ## Clean Maven build artifacts
	@echo "→ Cleaning build artifacts..."
	@mvn clean -q
	@echo "✓ Clean complete"

clients: build ## Generate clients.json with simulated client metadata
	@echo "→ Generating clients metadata..."
	docker exec $(JAVA_CONTAINER) java -cp $(JAVA_JAR) com.frauddetection.utils.ClientGenerator 100 /workspace/$(CLIENTS_FILE)
	@echo "✓ Clients generated -> $(CLIENTS_FILE)"

producer1: ## Run Producer1 via docker exec
	@echo "→ Running Producer1 via docker exec..."
	docker exec $(JAVA_CONTAINER) java -cp $(JAVA_JAR) com.frauddetection.producers.Producer1

consumer1: ## Run Consumer1 via docker exec
	@echo "→ Running Consumer1 via docker exec..."
	docker exec $(JAVA_CONTAINER) java -cp $(JAVA_JAR) com.frauddetection.consumers.Consumer1

consumer2: ## Run Consumer2 via docker exec
	@echo "→ Running Consumer2 via docker exec..."
	docker exec $(JAVA_CONTAINER) java -cp $(JAVA_JAR) com.frauddetection.consumers.Consumer2

consumer3: ## Run Consumer3 via docker exec
	@echo "→ Running Consumer3 via docker exec..."
	docker exec $(JAVA_CONTAINER) java -cp $(JAVA_JAR) com.frauddetection.consumers.Consumer3

run: ## Run CLASS=<java.class> with optional ARGS via docker exec
	@if [ -z "$(CLASS)" ]; then echo "Usage: make run CLASS=com.example.Main [ARGS='arg1 arg2']"; exit 1; fi
	@echo "→ Running $(CLASS) via docker exec..."
	docker exec $(JAVA_CONTAINER) sh -c "java -cp $(JAVA_JAR) $(CLASS) $(ARGS)"

.PHONY: help up down restart build clean clients producer1 consumer1 consumer2 consumer3 run
