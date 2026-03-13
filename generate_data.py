import pandas as pd
import numpy  as np
import hashlib, uuid, random
from datetime import datetime, timedelta

np.random.seed(42)
random.seed(42)

NUM_RECORDS = 1000          
NUM_CLIENTS = 200           
INDUSTRIES  = ['Banking', 'Healthcare', 'Government', 'IT']
START_DATE  = datetime(2024, 1, 1)

def generate_hash(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()


def random_timestamp(start: datetime, days: int = 365) -> datetime:
    return start + timedelta(
        days=random.randint(0, days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )




print("Generating Transaction Records dataSet")
records = []

for _ in range(NUM_RECORDS):

    client_id = f'CLIENT_{random.randint(1, NUM_CLIENTS):03d}'
    industry  = random.choice(INDUSTRIES)
    timestamp = random_timestamp(START_DATE)
    data_size = round(np.random.lognormal(mean=5, sigma=1.2), 2)       
    algo      = random.choice(['AES-256', 'RSA-2048', 'ChaCha20'])

    enc_ok    = np.random.choice([True, False], p=[0.9997, 0.0003])

    tps       = round(max(100, np.random.normal(loc=1200, scale=150)), 1)

    tampered  = np.random.choice([True, False], p=[0.005, 0.995])
    integrity = 0 if tampered else 1    

    block_hash = generate_hash(f'{client_id}{timestamp}{data_size}')
    prev_hash  = generate_hash(f'{client_id}{timestamp}')

    records.append({
        'tx_id'                : str(uuid.uuid4()),
        'client_id'            : client_id,
        'industry'             : industry,
        'timestamp'            : timestamp.strftime('%Y-%m-%d %H:%M'),
        'data_size_kb'         : data_size,
        'encryption_algorithm' : algo,
        'encryption_success'   : enc_ok,      
        'tps'                  : tps,          
        'tx_integrity'         : integrity,    
        'block_hash'           : block_hash,
        'prev_hash'            : prev_hash,
        'tamper_detected'      : tampered
    })

df_tx = pd.DataFrame(records)
print(f"   Done — {len(df_tx):,} records created.")



print("Generating System Monitoring Dataset")

monitoring = []

for i in range(365):
    date = (START_DATE + timedelta(days=i)).strftime('%Y-%m-%d')

    uptime = round(
        max(99.0, min(100.0, 100 - abs(np.random.normal(loc=0, scale=0.08)))), 4
    )

    unauth = max(0, int(np.random.poisson(lam=22)))
    alerts = max(0, int(np.random.poisson(lam=6)))

    response_min = round(abs(np.random.normal(loc=3.2, scale=1.0)), 2)

    nodes  = random.randint(48, 50)
    status = 'ALERT' if (uptime < 99.5 or unauth > 50) else 'Normal'

    monitoring.append({
        'date'                   : date,
        'uptime_pct'             : uptime,         
        'unauthorized_attempts'  : unauth,           
        'security_alerts'        : alerts,
        'incident_response_min'  : response_min,    
        'active_nodes'           : nodes,
        'status'                 : status
    })

df_system = pd.DataFrame(monitoring)
print(f"   Done — {len(df_system):,} daily records created.")




print("Generating CRM & Client Satisfaction Dataset")

clients = []

for i in range(1, NUM_CLIENTS + 1):
    industry = random.choice(INDUSTRIES)
    months   = random.randint(1, 12)

    satisfaction = round(
        max(1.0, min(10.0, np.random.normal(loc=8.4, scale=0.8))), 1
    )

    tickets   = max(0, int(np.random.poisson(lam=2)))

    renewal_p = round(max(0.1, min(0.99, 0.5 + (satisfaction - 5) * 0.09)), 2)

    clv       = round(3 * (1 + satisfaction / 5), 2)  

    # Churn risk category
    if satisfaction >= 8.0:
        risk = 'LOW'
    elif satisfaction >= 6.5:
        risk = 'MEDIUM'
    else:
        risk = 'HIGH'

    clients.append({
        'client_id'           : f'CLIENT_{i:03d}',
        'industry'            : industry,
        'subscription_months' : months,
        'satisfaction_score'  : satisfaction,   
        'support_tickets'     : tickets,
        'renewal_probability' : renewal_p,
        'clv_lakhs'           : clv,
        'churn_risk'          : risk
    })

df_clients = pd.DataFrame(clients)
print(f"   Done — {len(df_clients):,} client records created.")



df_tx.to_csv('blockshield_transactions.csv', index=False)
df_system.to_csv('blockshield_monitoring.csv', index=False)
df_clients.to_csv('blockshield_crm.csv', index=False)

print("  All 3 CSV files saved successfully!")

