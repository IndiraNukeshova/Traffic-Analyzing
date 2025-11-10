import json
import time
import pandas as pd
from kafka import KafkaProducer

# === 1. Настройки ===
TOPIC_NAME = "gps"
CSV_PATH = "taxi_data_subset.csv"   # убедись, что файл лежит в той же папке, что и скрипт
DELAY = 0.5  # задержка между сообщениями (секунды)

# === 2. Подключение к Kafka ===
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  # адрес брокера
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# === 3. Загружаем CSV ===
df = pd.read_csv(CSV_PATH)

# Переименуем колонки для удобства
df = df.rename(columns={
    "DriveNo": "taxi_id",
    "Latitude": "lat",
    "Longitude": "lon",
    "Date and Time": "timestamp"
})

# === 4. Отправляем данные по строкам ===
print("📤 Начинаю отправку сообщений в Kafka...")

for _, row in df.iterrows():
    message = {
        "taxi_id": int(row["taxi_id"]),
        "lat": float(row["lat"]),
        "lon": float(row["lon"]),
        "timestamp": str(row["timestamp"])
    }

    producer.send(TOPIC_NAME, value=message)
    print(f"📤 Sent: {message}")

    time.sleep(DELAY)  # имитация "реального времени"

producer.flush()
print("✅ All messages sent successfully.")
