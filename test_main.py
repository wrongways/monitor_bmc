import re
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt


def filter(df):
    """
    If # columns <= 4 return df as is, otherwise
    retain only the columns that have values that reach at least 60% of
    the column with the second highest max value
    """

    if len(df.columns) <= 4:
        return df

    max_values = df.max()
    penultimate_max = sorted(max_values)[-2]
    threshold = 0.6 * penultimate_max

    print(f'{max_values=}')
    print(f'{penultimate_max=}')
    print(f'{threshold=}')

    # Get a series of bools where the index is the column name
    pass_max = max_values > threshold

    # Get a list of the column names
    idx = pass_max.index

    # Filter the list of column names if pass_max is true for column
    pass_cols = [idx[i] for i, v in enumerate(pass_max) if v]

    # return dataframe with the passing columns
    return df[pass_cols]


files = list(Path('../samples').glob('oahu10012*.pkl'))
for f in files:
    print(f)
dataframes = {re.match(r'.*_(\w+)_.*', str(f)).group(1): pd.read_pickle(f) for f in files}

print(len(dataframes))
print(dataframes.keys())
print(dataframes['Power'].head())
sensors = dataframes['Sensors']
sensors.index = sensors.index / 60
sensors.index.name = 'minutes'
print(sensors.head())
power_sensors = sensors[
    [col for col in sensors.columns if 'pwr' in col.lower() or 'power' in col.lower()]
]
temp_sensors = sensors[[col for col in sensors.columns if 'temp' in col.lower()]]
print('Power Sensors')
print(power_sensors.head())
top_power = filter(dataframes['Power'])
plot_n_rows = int(round(len(top_power.columns) / 2))
top_power.index = top_power.index / 60
top_power.index.name = 'Minutes'
# top_power = top_power.assign(minutes=top_power.index / 60)


fig1, axes1 = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
fig2, axes2 = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
top_power.plot(
    title='Top power draws for system',
    ylabel='Power (Watts)',
    xlabel='Time (Minutes)',
    ax=axes1[0],
)

temps = dataframes['Temperatures']

max_temps = temps.max()
threshold = sorted(max_temps)[-5]
pass_max = max_temps >= threshold
idx = pass_max.index
top_4_temps = [idx[i] for i, v in enumerate(pass_max) if v]

print('*' * 90)
print(f'{pass_max=}')
print(f'{max_temps=}')
print(f'{threshold=}')
print(f'{top_4_temps=}')

temps.index = dataframes['Temperatures'].index / 60
temps[top_4_temps].plot(
    title='Temperatures', xlabel='Time (Minutes)', ylabel='Temperature (℃)', ax=axes1[1]
)


max_temps = temp_sensors.max()
threshold = sorted(max_temps)[-4]
pass_max = max_temps >= threshold
idx = pass_max.index
top_4_temps = {idx[i] for i, v in enumerate(pass_max) if v}
print(f"{re.match(r'CPU\d+[_ ]TEMP', 'CPU22_TEMP') is not None=}")
cpu_temp_cols = {
    col for col in temp_sensors.columns if re.match(r'CPU\d+_TEMP', col) is not None
}
print(f'{cpu_temp_cols=}')
cols = sorted(top_4_temps | cpu_temp_cols)
print(f'{cols=}')
power_sensors = filter(power_sensors)
power_sensors.plot(ax=axes2[0])
temp_sensors[cols].plot(ax=axes2[1])
plt.show()
plt.close()

for name, value in temp_sensors.max().sort_values(ascending=False)[:5].items():
    print(f'{name:>20} {value:5.1f}')


ESC = '\033['
RED = f'{ESC}38;5;196m'
GREEN = f'{ESC}38;5;118m'
RESET = f'{ESC}0m'

print(f'{RED}red{RESET} {GREEN}green{RESET} reset')


# Fans
