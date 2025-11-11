# reset_env.py
from kafka.admin import KafkaAdminClient, NewTopic
import duckdb
import time

# ======= Очистка Kafka =======
admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

# Удаляем топик gps (если есть)
try:
    admin.delete_topics(["gps"])
    print("🗑️ Топик 'gps' удалён")
    # Иногда нужно подождать пару секунд
    time.sleep(3)
except Exception as e:
    print("⚠️ Не удалось удалить топик или он уже удалён:", e)

# Создаём топик заново
try:
    topic = NewTopic(name="gps", num_partitions=1, replication_factor=1)
    admin.create_topics([topic])
    print("✅ Топик 'gps' создан заново")
except Exception as e:
    print("⚠️ Ошибка при создании топика:", e)

# ======= Очистка DuckDB =======
conn = duckdb.connect("taxi_data.duckdb")
conn.execute("DROP TABLE IF EXISTS gps_data")
conn.execute("DROP TABLE IF EXISTS taxi_aggregates")
conn.commit()
conn.close()
print("🗑️ DuckDB очищен (gps_data и taxi_aggregates)")
