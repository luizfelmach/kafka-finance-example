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
        print(f"Erro no evento de Auth: {err}")
    else:
        print(f"Evento de Auth enviado: {msg.topic()}")

def generate_auth_event():
    user_ids = ['user_123', 'user_456', 'user_789']
    event_types = ['login', 'logout', 'password_change', 'mfa_challenge']
    
    status = "fail" if random.random() < 0.2 else "success"

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "event_type": random.choice(event_types),
        "status": status,
        "ip_address": f"192.168.1.{random.randint(1, 254)}",
        "device_id": f"dev_{random.randint(1000, 9999)}",
        "timestamp": datetime.now().isoformat()
    }
    return event

print("Iniciando Produtor de Autenticação...")

try:
    while True:
        data = generate_auth_event()
        producer.produce(
            'auth.events', 
            key=data['user_id'], 
            value=json.dumps(data).encode('utf-8'), 
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(random.uniform(5, 10))
except KeyboardInterrupt:
    pass
finally:
    producer.flush()