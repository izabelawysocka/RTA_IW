from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(list)

print("Nasłuchuję...")

for message in consumer:
    tx = message.value
    user = tx['user_id']
    timestamp = datetime.fromisoformat(tx['timestamp'])

    user_transactions[user].append(timestamp)

    # tylko ostatnie 60 sekund
    user_transactions[user] = [
        t for t in user_transactions[user]
        if (timestamp - t).total_seconds() <= 60
    ]

    if len(user_transactions[user]) > 3:
        print(f"🚨 ALERT: {user} zrobił {len(user_transactions[user])} transakcji w 60 sekund!")
        