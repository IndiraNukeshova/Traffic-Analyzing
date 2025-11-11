import duckdb

# Подключаемся к БД
conn = duckdb.connect("taxi_data.duckdb")

# Агрегация по часу
query = """
CREATE OR REPLACE TABLE taxi_aggregates AS
SELECT 
    DATE_TRUNC('hour', timestamp) AS hour,
    COUNT(*) AS total_points,
    COUNT(DISTINCT taxi_id) AS unique_taxis,
    MIN(timestamp) AS earliest,
    MAX(timestamp) AS latest,
    AVG(lat) AS avg_lat,
    AVG(lon) AS avg_lon
FROM gps_data
GROUP BY 1
ORDER BY 1;
"""
conn.execute(query)
print("✅ Aggregation completed and saved to 'taxi_aggregates'")

# Проверим результат
result = conn.execute("SELECT * FROM taxi_aggregates LIMIT 5").fetchdf()
print(result)
conn.close()
