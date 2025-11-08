import json
import duckdb
from kafka import KafkaConsumer
import pandas as pd

# === 1. Настройка Kafka consumer ===
consumer = KafkaConsumer(
    'gps',  # топик
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # читать с начала
    enable_auto_commit=True,       # подтверждать прочтение автоматически
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # JSON → dict
    group_id=None,  # без группы = не сохраняет позицию
)

# === 2. Подключение к DuckDB ===
conn = duckdb.connect('gps_data.duckdb')  # создаст файл базы, если не существует

# Создаём таблицу, если её нет
conn.execute('''
CREATE TABLE IF NOT EXISTS gps_raw (
    taxi_id INTEGER,
    lat DOUBLE,
    lon DOUBLE,
    timestamp VARCHAR
)
''')

print("👀 Consumer started. Waiting for messages...")

# === 3. Чтение сообщений и запись в DuckDB ===
for message in consumer:
    record = message.value
    df = pd.DataFrame([record])  # превращаем dict → DataFrame
    conn.execute("INSERT INTO gps_raw SELECT * FROM df")
    print(f"💾 Saved: {record}")
