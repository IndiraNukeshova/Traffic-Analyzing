import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# 1️⃣ Подключаемся к БД
conn = duckdb.connect("taxi_data.duckdb")

# 2️⃣ Читаем таблицу с агрегированными данными
df = conn.execute("SELECT * FROM taxi_aggregates ORDER BY interval_10min").fetchdf()
conn.close()

print("✅ Loaded aggregated data:")
print(df.head())

# 3️⃣ Преобразуем колонку времени, если нужно
#df['interval_10min'] = pd.to_datetime(df['interval_10min'])

# 4️⃣ Строим графики
plt.figure(figsize=(12, 6))
plt.plot(df['interval_10min'], df['total_points'], marker='o', label='Total Points')
plt.plot(df['interval_10min'], df['unique_taxis'], marker='x', label='Unique Taxis')
plt.title('Taxi Activity by every 10 minutes')
plt.xlabel('Time Interval')
plt.ylabel('Count')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# 5️⃣ Если есть координаты, можно построить тепловую карту
if 'avg_lat' in df.columns and 'avg_lon' in df.columns:
    plt.figure(figsize=(8, 6))
    plt.scatter(df['avg_lon'], df['avg_lat'], c=df['total_points'], cmap='viridis', s=80)
    plt.title('Average Taxi Positions (weighted by activity)')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.colorbar(label='Total Points')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
