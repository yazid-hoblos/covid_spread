# experiments/analyze_summary.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

os.makedirs('plots', exist_ok=True)
df = pd.read_csv('experiments/results/results_raw2.csv')
# normalize numeric columns
df['wall_time_sec'] = pd.to_numeric(df['wall_time_sec'], errors='coerce')
df['max_rss_kb'] = pd.to_numeric(df['max_rss_kb'], errors='coerce')

# optional: filter out clearly-bad runs (wall_time <= 0.5s)
bad = df[df['wall_time_sec'] <= 0.5]
if not bad.empty:
    print('Warning: very small runs found (inspect logs):')
    print(bad[['framework','workload','runid','wall_time_sec','logfile']])

# aggregated stats
agg = df.groupby(['framework','workload']).agg(
    n=('wall_time_sec','count'),
    median=('wall_time_sec','median'),
    iqr=('wall_time_sec', lambda x: np.percentile(x,75)-np.percentile(x,25)),
    mean=('wall_time_sec','mean'),
    std=('wall_time_sec','std'),
    median_rss_kb=('max_rss_kb','median')
).reset_index()
agg.to_csv('experiments/results/summary2.csv', index=False)
print('Wrote experiments/results/summary2.csv')

# boxplot runtime
plt.figure(figsize=(10,6))
df['fw_wk'] = df['framework'] + '_' + df['workload']
order = sorted(df['fw_wk'].unique())
df.boxplot(column='wall_time_sec', by='fw_wk', grid=False, rot=45)
plt.title('Runtime (s) by framework_workload')
plt.suptitle('')
plt.ylabel('seconds')
plt.tight_layout()
plt.savefig('plots/runtime_boxplot2.png', dpi=150)
print('Wrote plots/runtime_boxplot2.png')

# boxplot memory
plt.figure(figsize=(10,6))
df.boxplot(column='max_rss_kb', by='fw_wk', grid=False, rot=45)
plt.title('Max RSS (KB) by framework_workload')
plt.suptitle('')
plt.ylabel('KB')
plt.tight_layout()
plt.savefig('plots/memory_boxplot2.png', dpi=150)
print('Wrote plots/memory_boxplot2.png')