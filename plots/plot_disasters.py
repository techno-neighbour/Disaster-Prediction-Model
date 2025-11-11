# plot_disasters.py
# Requires: python3, matplotlib, pandas (install via pip if needed)
import pandas as pd
import matplotlib.pyplot as plt

# daily counts plot
df = pd.read_csv('../output/daily_counts.csv')
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date')
plt.figure(figsize=(10,4))
plt.plot(df['date'], df['count'], marker='o')
plt.title('Daily Event Counts')
plt.xlabel('Date'); plt.ylabel('Count')
plt.grid(True)
plt.tight_layout()
plt.savefig('../output/daily_counts_plot.png')
print('Saved daily_counts_plot.png')

# hotspots bar (top 20)
h = pd.read_csv('../output/hotspots.csv')
h = h.sort_values('count', ascending=False).head(20)
plt.figure(figsize=(10,6))
plt.barh(range(len(h)), h['count'][::-1])
plt.yticks(range(len(h)), h['lat_bin'].astype(str)[::-1] + ',' + h['lon_bin'].astype(str)[::-1])
plt.title('Top 20 Hotspot Bins (lat_bin,lon_bin)')
plt.tight_layout()
plt.savefig('../output/hotspots_top20.png')
print('Saved hotspots_top20.png')
