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
from likan_thjalfun import thjalfa_likan
from api_gogn import *

uppfaera_allt()
#%%
def keyra_likan(stod='Reykjavik', efni = 'NO2'):
    stodisl = stod
    if stod == 'Reykjavik':
        stodisl = 'Reykjavík'
    prev = efni+'_prev'
    # Hlaða inn spágögnum og líkankjarna
    inp = pd.read_csv('..\\data\\vedurspa.csv', encoding = "ISO-8859-1", sep = ',')
    means = pd.read_pickle('..\models\means_'+stod+'_'+efni)
    stds = pd.read_pickle('..\models\stds_'+stod+'_'+efni)
    nn = keras.models.load_model('..\models\likan_'+stod+'_'+efni)
    # Umbreyta formi gagna eftir þörfum
    inp['datetime'] = pd.to_datetime(inp['Spátími'], format='%Y-%m-%d %H:00:00')
    inp = inp.set_index(['datetime'])
    inp = inp[inp.Nafn == stodisl]
    inp = inp.resample('60T').interpolate('linear')
    inp['datetime'] = inp.index
    t0 = inp.index[0]
    inp['ws'] = inp['Vindhraði (m/s)']
    inp['wd'] = inp['Vindgráður']
    inp = inp.head(168)

    # Hlaða inn mæligögnum og matreiða
    if stod == 'Reykjavik':
        df = pd.read_csv('..\data\saman_1475_STA-IS0005A.csv', sep=";")
    if stod == 'Akureyri':
        df = pd.read_csv('..\data\saman_3471_STA-IS0052A.csv', sep=";")
    df['datetime'] = pd.to_datetime(df['endtime'], format='%Y-%m-%d %H:00:00')
    df['datetime'] = pd.DatetimeIndex(df['datetime'])
    df = df.set_index(['datetime'])
    df = df.resample('60T').sum()
    df[efni].loc[df[efni] > 500] = np.nan
    df[efni].loc[df[efni] == 0] = np.nan

    tmp = df[efni].loc[((df.index < t0) & (df.index >= (t0-pd.DateOffset(days=10))))]
    tmp = tmp.resample('60T').interpolate('linear')
    tmp.index = tmp.index+pd.DateOffset(days=7)
    inp = inp.join(tmp)
    inp[prev] = inp[efni]
    inp = inp[['ws','wd',prev]]
    inp['datetime'] = inp.index
    # Skapa tímabreytur
    inp['n_hr'] = inp.datetime.dt.hour
    inp['n_wd'] = inp.datetime.dt.weekday
    inp['n_yd'] = inp.datetime.dt.day
    # Skipta upp í componenta
    inp['n_hr_x'] = np.cos(inp.n_hr*np.pi/12)
    inp['n_hr_y'] = np.sin(inp.n_hr*np.pi/12)
    inp['n_wd_x'] = np.cos(inp.n_wd*np.pi/3.5)
    inp['n_wd_y'] = np.sin(inp.n_wd*np.pi/3.5)
    inp['n_yd_x'] = np.cos(inp.n_yd*np.pi/183)
    inp['n_yd_y'] = np.sin(inp.n_yd*np.pi/183)
    inp['wd_x'] = np.cos(inp.wd*np.pi/180)
    inp['wd_y'] = np.sin(inp.wd*np.pi/180)

    # Endurraða, síðasti verður mark-dálkurinn, henda burt óþarfa gögnum
    inp = inp[['datetime','n_yd_x','n_yd_y',
            'n_wd_x','n_wd_y','n_hr_x','n_hr_y',
            'wd_x','wd_y','ws',prev]]
    cols = ['n_yd_x','n_yd_y',
            'n_wd_x','n_wd_y','n_hr_x','n_hr_y',
            'wd_x','wd_y','ws',prev]#['ws','PM10_ystd','PM10']
    # Normalisera
    for col in cols:
        inp[col] = (inp[col]-means[col])/stds[col]

    pred = pd.DataFrame(nn.predict(inp.drop(columns='datetime')).flatten()*stds[efni]+means[efni], columns=['gildi'])
    pred.index = inp.index
    return pred

# Búa til spár fyrir Reykjavík og Akureyri, PM10 og NO2
rvk_pm10 = keyra_likan(stod='Reykjavik', efni = 'PM10')
rvk_pm10['efni'] = 'PM10'
rvk_pm10['station_local_id'] = 'Reykjavik'
rvk_no2 = keyra_likan(stod='Reykjavik', efni = 'NO2')
rvk_no2['efni'] = 'NO2'
rvk_no2['station_local_id'] = 'Reykjavik'
ak_pm10 = keyra_likan(stod='Akureyri', efni = 'PM10')
ak_no2['efni'] = 'PM10'
ak_no2['station_local_id'] = 'Akureyri'
ak_no2 = keyra_likan(stod='Akureyri', efni = 'NO2')
ak_no2['efni'] = 'NO2'
ak_no2['station_local_id'] = 'Akureyri'

# Sameina úttök líkans og prenta út í eina csv skrá sem matast inn í Node.red
out = rvk_pm10
out = out.append(rvk_no2)
out = out.append(rvk_pm10)
out = out.append(ak_no2)
out.gildi = out.gildi.round(decimals=0)
out.index = out.index.strftime('%m-%d %H:00')

out.to_csv('..\data\output_full.csv')
