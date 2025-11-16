import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import numpy as np

# Подключаемся
conn = duckdb.connect("taxi_data.duckdb")

# Загружаем все gps точки
df = conn.execute("SELECT lat, lon FROM gps_data").fetchdf()
conn.close()

plt.figure(figsize=(8, 6))
plt.hexbin(df["lon"], df["lat"], gridsize=40)
plt.title("Heatmap of Taxi GPS Points (Hexbin)")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.colorbar(label="Density")
plt.tight_layout()
plt.show()
