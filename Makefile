# Makefile - Kafka Finance Example
# Comandos utilitários para gerenciar o projeto de detecção de fraudes com Kafka

COMPOSE_FILE := compose.yaml

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo ""
	@echo "Kafka Finance Example"
	@echo "Main project commands"
	@echo ""
	@echo "Usage: make <target> [VAR=value]"
	@echo ""
	@echo "Compose"
	@printf "  \033[36m%-14s\033[0m %s\n" "up" "Start all services with Docker Compose (builds image if needed)"
	@printf "  \033[36m%-14s\033[0m %s\n" "down" "Stop and remove all Docker Compose services"
	@printf "  \033[36m%-14s\033[0m %s\n" "restart" "Restart all services (recreates containers to pick up new env vars)"
	@echo ""
	@echo "Examples:"
	@echo "  make up"
	@echo "  make down"
	@echo "  make restart"
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

.PHONY: help up down restart
