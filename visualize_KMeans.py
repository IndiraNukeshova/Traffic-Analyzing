import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import numpy as np

# загружаем снова
conn = duckdb.connect("taxi_data.duckdb")
df = conn.execute("SELECT lat, lon FROM gps_data").fetchdf()
conn.close()

coords = df[['lat', 'lon']].values

# выберем 5 кластеров (можно менять)
kmeans = KMeans(n_clusters=5, n_init="auto")
kmeans.fit(coords)

df['cluster'] = kmeans.labels_

plt.figure(figsize=(8, 6))
plt.scatter(df["lon"], df["lat"], c=df["cluster"], s=10)
plt.title("KMeans Clusters of Taxi GPS Points")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.tight_layout()
plt.show()
