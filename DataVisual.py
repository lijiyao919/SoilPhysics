import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

df = pd.read_csv('Soil_2017-09-25T00-00.csv', nrows=50)
df.replace(-999999, -0.1, inplace=True)

sm_2 = list(df['sm_2'])
sm_4 = list(df['sm_4'])
sm_8 = list(df['sm_8'])
sm_20 = list(df['sm_20'])
sm_40 = list(df['sm_40'])

print(sm_4)

fig, ax = plt.subplots()
index = np.arange(50)
bar_width = 0.1

rects1 = plt.bar(index, sm_2, bar_width, color='b', label='sm_2')
rects2 = plt.bar(index + bar_width, sm_4, bar_width, color='r', label='sm_4')
rects3 = plt.bar(index + 2*bar_width, sm_8, bar_width, color='g', label='sm_8')
rects4 = plt.bar(index + 3*bar_width, sm_20, bar_width, color='y', label='sm_20')
rects5 = plt.bar(index + 4*bar_width, sm_40, bar_width, color='pink', label='sm_40')

plt.xlabel('Station')
plt.ylabel('Moisture')
plt.title('Soil Moisuture')
plt.xticks(index + bar_width, range(1,51))
plt.legend()

plt.tight_layout()
plt.show()