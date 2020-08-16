#%%
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from keras.models import Sequential
from keras.layers import *
from keras.optimizers import SGD, Adam
import keras
import tensorflow as tf
import keras.backend as kb

#%%
# Hlaða inn gögnum
df_full = pd.read_csv('Data_isl\grensasv_cleaned.csv')
df_full['datetime'] = pd.to_datetime(df_full['datetime'], format='%Y-%m-%d %H:00:00')
df = df_full[['datetime','ws','wd','PM10']]
#### TO-DO: Hreinsa gögn (PM10 fer upp í 990 o.s.frv.)

# Skapa tímabreytur
df['n_hr'] = df.datetime.dt.hour
df['n_wd'] = df.datetime.dt.weekday
df['n_yd'] = df.datetime.dt.day
# Skipta upp í componenta
df['n_hr_x'] = np.cos(df.n_hr*np.pi/12)
df['n_hr_y'] = np.sin(df.n_hr*np.pi/12)
df['n_wd_x'] = np.cos(df.n_wd*np.pi/3.5)
df['n_wd_y'] = np.sin(df.n_wd*np.pi/3.5)
df['n_yd_x'] = np.cos(df.n_yd*np.pi/183)
df['n_yd_y'] = np.sin(df.n_yd*np.pi/183)
df['wd_x'] = np.cos(df.wd*np.pi/180)
df['wd_y'] = np.sin(df.wd*np.pi/180)

# Gera "gærdags" dálk
df['PM10_ystd'] = df['PM10'].shift(+24)
df['PM10_ystd'] = df['PM10_ystd'].fillna(method='bfill')
# Endurraða, síðasti verður mark-dálkurinn, henda burt óþarfa gögnum
df = df[['datetime','n_yd_x','n_yd_y',
         'n_wd_x','n_wd_y','n_hr_x','n_hr_y',
         'wd_x','wd_y','ws','PM10_ystd','PM10']]
cols = ['ws','PM10_ystd','PM10']
# Normalisera
df[cols] = df[cols] .apply(lambda x: (x - x.mean()) / x.std())
df.head()

#%%
# Skipta gagnasetti upp í train, validation og test set.
# X_Train, y_Train, X_Valid, y_Valid, X_Test, y_Test, mean, std, AllData, AllDataWithForecast

X_DATA = df
mask = (df['datetime'] < '2020-03-06')
X_Train = df.loc[mask].drop(columns=['datetime','PM10'])
y_Train = df.loc[mask,'PM10']#.drop(columns=['datetime'])
#
mask = (df['datetime'] < '2020-05-01') & (df['datetime'] >= '2020-03-06')
X_Valid = df.loc[mask].drop(columns=['datetime','PM10'])
y_Valid = df.loc[mask,'PM10']#.drop(columns=['datetime'])
#
mask = (df['datetime'] >= '2020-05-01')
X_Test = df.loc[mask].drop(columns=['datetime','PM10'])
y_Test = df.loc[mask,'PM10']#.drop(columns=['datetime'])

#%%
# Setja upp neural network líkan
nn = Sequential([Dense(256, input_shape=(X_Train.shape[1],), activation="relu"), Dense(1, activation="linear")])
nn.summary()
nn.compile(Adam(1e-1), "mse")
# Þjálfa
nn.fit(X_Train, y_Train, 1024, epochs=28, validation_data=(X_Valid, y_Valid))
print_scores(nn)
#%%
op = df.join(pd.DataFrame(nn.predict(df.drop(columns=['datetime','PM10'])).flatten(), columns=["NN_PM10"]))
mask = (op.datetime < '2020-02-01') & (op.datetime > '2020-01-03')

plt.figure()
ax=plt.gca()
plt.plot(op.datetime.loc[mask],op.PM10.loc[mask])
plt.plot(op.datetime.loc[mask],op.NN_PM10.loc[mask])