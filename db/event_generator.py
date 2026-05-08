import pandas as pd
import random
import uuid
import os
from datetime import datetime, timedelta

name = str(input("Digite um nome para o arquivo de eventos (sem extensão): ")).strip()

TOTAL_CLIENTS = 100
MAX_ACCOUNTS_PER_CLIENT = 2  
MAX_DEVICES_PER_CLIENT = 2
DAYS_TO_GENERATE = 6
BASE_DATE = datetime(2026, 5, 4, 0, 0, 0)

#Volume diário de eventos
NORMAL_TX_VOLUME = 100   
NORMAL_AUTH_VOLUME = 20  
NORMAL_FAILED_AUTH_VOLUME = 5
NORMAL_DEVICE_CHANGE_VOLUME = 2

# uantidade de clientes envolvidos nas situações de interesse por dia
S2_VALOR_ALTO_QTY = 5
S3_ALTA_FREQ_QTY = 5
S4_DEVICE_NOVO_QTY = 5
S5_SENHA_TX_QTY = 5
S6_FRAUDE_COMPLEXA_QTY = 2

#Para definir o valor máximo de transações normais (baseado na média do cliente)
NORMAL_TRANSACTION_MULTIPLIER = 1.5 

#Para definir um valor discrepante de transações (baseado na média do cliente)
HIGH_TRANSACTION_MULTIPLIER = 20

#--- SIMULAÇÃO DE CLIENTES ---
clients = []
for i in range(TOTAL_CLIENTS):
    u_id = f"u-{i:03}"
    trusted_devs = [f"dev-{u_id}-{j}" for j in range(random.randint(1, MAX_DEVICES_PER_CLIENT))]
    accounts = [f"acc-{u_id}-{j}" for j in range(random.randint(1, MAX_ACCOUNTS_PER_CLIENT))]
    
    clients.append({
        "user_id": u_id,
        "accounts": accounts,
        "trusted_devices": trusted_devs,
        "avg_amt": random.uniform(100, 700),
        "home_ip": f"177.10.{random.randint(0,255)}.{random.randint(0,255)}"
    })

all_accounts = [acc for c in clients for acc in c["accounts"]]

#--- GERAÇÃO DE EVENTOS ---
consolidated_events = []

for day in range(DAYS_TO_GENERATE):
    day_start = BASE_DATE + timedelta(days=day)
    
    def add_event(e_type, user, ts, **kwargs):
        acc_id = kwargs.get("account_id", random.choice(user["accounts"]))
        dest_acc = None
        if e_type == "TRANSACTION":
            dest_acc = random.choice([acc for acc in all_accounts if acc != acc_id])
            
        event = {
            "event_id": str(uuid.uuid4())[:8],
            "event_type": e_type,
            "timestamp": ts,
            "transaction_type": random.choice(["DEBIT", "CREDIT", "PIX"]) if e_type == "TRANSACTION" else None,
            "user_id": user["user_id"],
            "device_id": kwargs.get("device_id", random.choice(user["trusted_devices"])),
            "ip_address": kwargs.get("ip", user["home_ip"]),
            "account_id": acc_id,
            "destination_account": dest_acc,
            "amount": kwargs.get("amount", None),
            "status": kwargs.get("status", "COMPLETED" if e_type != "TRANSACTION" else "REQUESTED")
        }
        consolidated_events.append(event)

    #EVENTOS NORMAIS (Ocorrem todos os dias para clientes aleatórios)
    for _ in range(NORMAL_TX_VOLUME):
        c = random.choice(clients)
        ts = day_start + timedelta(seconds=random.randint(0, 86399))
        add_event("TRANSACTION", c, ts, amount=round(random.uniform(10, c["avg_amt"]*NORMAL_TRANSACTION_MULTIPLIER), 2))
        
    for _ in range(NORMAL_AUTH_VOLUME):
        c = random.choice(clients)
        ts = day_start + timedelta(seconds=random.randint(0, 86399))
        add_event("LOGIN", c, ts)

    for _ in range(NORMAL_FAILED_AUTH_VOLUME):
        c = random.choice(clients)
        ts = day_start + timedelta(seconds=random.randint(0, 86399))
        add_event("FAILED_AUTH", c, ts, status="FAILED")

    for _ in range(NORMAL_DEVICE_CHANGE_VOLUME):
        c = random.choice(clients)
        ts = day_start + timedelta(seconds=random.randint(0, 86399))
        new_dev = f"dev-{c['user_id']}-{len(c['trusted_devices'])}"
        c['trusted_devices'].append(new_dev)
        add_event("DEVICE_CHANGE", c, ts, device_id=new_dev)
    
    #DIA 2: Valores muito acima do padrão
    if day == 1:
        for c in random.sample(clients, S2_VALOR_ALTO_QTY):
            ts = day_start + timedelta(hours=random.randint(10, 20))
            add_event("TRANSACTION", c, ts, amount=round(c["avg_amt"] * HIGH_TRANSACTION_MULTIPLIER, 2))

    #DIA 3: Muitas transações em janela curta
    elif day == 2:
        for c in random.sample(clients, S3_ALTA_FREQ_QTY):
            start_ts = day_start + timedelta(hours=random.randint(10, 20))
            for i in range(10): # 10 transações em 30 segundos
                add_event("TRANSACTION", c, start_ts + timedelta(seconds=i*3), amount=random.randint(100, 10000))

    #DIA 4: Dispositivos não reconhecidos
    elif day == 3:
        for c in random.sample(clients, S4_DEVICE_NOVO_QTY):
            ts = day_start + timedelta(hours=random.randint(10, 20))
            add_event("TRANSACTION", c, ts, device_id="UNRECOGNIZED-HW-99", amount=300.00)

    #DIA 5: Alteração de senha + Transação relevante
    elif day == 4:
        for c in random.sample(clients, S5_SENHA_TX_QTY):
            ts_base = day_start + timedelta(hours=random.randint(10, 20))
            add_event("PASSWORD_CHANGE", c, ts_base)
            add_event("TRANSACTION", c, ts_base + timedelta(minutes=4), amount=1000.00)

    #DIA 6: Evento Derivado
    elif day == 5:
        for c in random.sample(clients, S6_FRAUDE_COMPLEXA_QTY):
            ts_f = day_start + timedelta(hours=random.randint(10, 20), minutes=random.randint(0, 59))
            ip_atq = "190.2.1.50"
            dev_atq = "HACKER-PAD-01"
            #Tentativas falhas antes do login
            for i in range(3):
                add_event("FAILED_AUTH", c, ts_f - timedelta(minutes=5-i), device_id=dev_atq, ip=ip_atq, status="FAILED")
                
            add_event("LOGIN", c, ts_f, device_id=dev_atq, ip=ip_atq)
            #Transação suspeita logo após o login
            add_event("PASSWORD_CHANGE", c, ts_f + timedelta(seconds=30), device_id=dev_atq, ip=ip_atq)
            add_event("TRANSACTION", c, ts_f + timedelta(minutes=2), device_id=dev_atq, ip=ip_atq, amount=7000.00)

    print(f"Dia {day+1} ({day_start.strftime('%Y-%m-%d')}) gerado...")

# Salva o arquivo consolidado
df_all = pd.DataFrame(consolidated_events)
df_all = df_all.sort_values(by='timestamp').reset_index(drop=True)
events_filename = os.path.join('.', f"all_events_{name}.csv")
df_all.to_csv(events_filename, index=False)
print(f"\nTotal de {len(df_all)} eventos salvos em '{events_filename}'.")

profiles_filename = os.path.join('.', f"customer_profiles_{name}.csv")
pd.DataFrame(clients).to_csv(profiles_filename, index=False)
print(f"Perfis de clientes salvos em '{profiles_filename}'.")