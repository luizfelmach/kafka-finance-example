#!/bin/bash

echo "Aguardando o Kafka (kafka:29092)..."

while ! kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "Kafka ainda não disponível. Tentando novamente em 5 segundos..."
  sleep 5
done

echo "Kafka online. Criando tópicos para detecção de fraude..."

TOPICS=(
  "transactions.raw"
  "auth.events"
  "customer.profile"
  "fraud.alerts"
  "risk.scores"
  "transactions.blocked"
  "transactions.approved"
  "transactions.review"
)

for TOPIC in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 1
  echo "Tópico configurado: $TOPIC"
done

echo "Infraestrutura inicializada."