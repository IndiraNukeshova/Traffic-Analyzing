import json
import time
import pandas as pd
from kafka import KafkaProducer

# === 1. Подключение к Kafka ===
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # адрес брокера
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # превращаем dict → JSON → bytes
)

# === 2. Загружаем CSV ===
# Можно использовать свой файл, например "rome_taxi_sample.csv"
# Для теста создадим DataFrame вручную:
data = pd.DataFrame([
    {"taxi_id": 1, "lat": 41.9028, "lon": 12.4964, "timestamp": "2025-11-06T10:00:00"},
    {"taxi_id": 1, "lat": 41.9030, "lon": 12.4970, "timestamp": "2025-11-06T10:00:05"},
    {"taxi_id": 2, "lat": 41.8902, "lon": 12.4922, "timestamp": "2025-11-06T10:00:10"},
])

# === 3. Отправляем строки в Kafka ===
for _, row in data.iterrows():
    message = row.to_dict()
    producer.send('gps', value=message)
    print(f"📤 Sent: {message}")
    time.sleep(1)  # имитируем "живой" поток данных

# === 4. Завершаем ===
producer.flush()
print("✅ All messages sent successfully.")
