import pandas as pd
import matplotlib.pyplot as plt

# Load CSV
df = pd.read_csv("data/data.csv", usecols=["dateRep"])

# Count records per date
daily_counts = df['dateRep'].value_counts().sort_index(key=lambda x: pd.to_datetime(x, format='%d/%m/%Y'))

# Plot
plt.figure(figsize=(12,5))
daily_counts.plot(kind='line', marker='o')
plt.xlabel("Date")
plt.ylabel("Number of records")
plt.title("Number of Records per Day in the COVID-19 Dataset")
plt.grid(True)
plt.tight_layout()
plt.savefig("figures/daily_records_trend.png")
plt.show()

