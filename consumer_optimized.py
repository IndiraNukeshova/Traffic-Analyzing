from kafka import KafkaConsumer
import json
import duckdb
import time

# Подключаемся к Kafka
consumer = KafkaConsumer(
    'gps',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # читать даже старые сообщения
    group_id='gps-group-1',        # уникальное имя группы
    enable_auto_commit=True
)

print("👀 Consumer started (optimized). Waiting for messages...")

# Подключаемся к базе
conn = duckdb.connect('taxi_data.duckdb')

# Создаём таблицу, если нет
conn.execute("""
CREATE TABLE IF NOT EXISTS gps_data (
    taxi_id INTEGER,
    lat DOUBLE,
    lon DOUBLE,
    timestamp TIMESTAMP
)
""")

# Настройки буфера
batch = []
batch_size = 3          # сохраняем каждые 3 сообщения
flush_interval = 5      # или если прошло 5 секунд
last_flush = time.time()

# Основной цикл
for message in consumer:
    data = message.value
    batch.append((
        data['taxi_id'],
        data['lat'],
        data['lon'],
        data['timestamp']
    ))

    hi

    # Проверяем, пора ли сбросить данные
    if len(batch) >= batch_size or (time.time() - last_flush) >= flush_interval:
        conn.executemany(
            "INSERT INTO gps_data VALUES (?, ?, ?, ?)",
            batch
        )
        conn.commit()
        print(f"💾 Saved {len(batch)} rows to DuckDB")
        batch = []
        last_flush = time.time()
