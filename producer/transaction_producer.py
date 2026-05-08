import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Falha ao entregar mensagem: {err}")
    else:
        print(f"Transação enviada: {msg.topic()} [{msg.partition()}]")

def generate_transaction():
    user_ids = ['user_123', 'user_456', 'user_789', 'user_000']
    categories = ['retail', 'entertainment', 'food', 'electronics']
    
    # Simula uma transação fraudulenta ocasional (valor alto)
    is_suspicious = random.random() < 0.1
    amount = random.uniform(10000, 50000) if is_suspicious else random.uniform(10, 500)

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "amount": round(amount, 2),
        "currency": "BRL",
        "merchant_id": f"mct_{random.randint(1, 100)}",
        "merchant_category": random.choice(categories),
        "timestamp": datetime.now().isoformat(),
        "card_type": random.choice(["credit", "debit", "virtual"]),
        "location": {
            "lat": -20.3155,
            "lon": -40.3128,
            "city": "Vitoria"
        }
    }
    return transaction

print("Iniciando Produtor de Transações (Ctrl+C para parar)...")

try:
    while True:
        data = generate_transaction()
        producer.produce(
            'transactions.raw', 
            key=data['user_id'], 
            value=json.dumps(data).encode('utf-8'), 
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(random.uniform(1, 3))
except KeyboardInterrupt:
    print("\nStopping...")
finally:
    producer.flush()