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

def thjalfa_likan(stod='Reykjavik',efni='PM10',plot_diagnostics=False):

    prev = efni+'_prev'
    # Hlaða inn gögnum
    if stod == 'Reykjavik':
        df_full = pd.read_csv('..\data\saman_1475_STA-IS0005A.csv', sep=";")
    if stod == 'Akureyri':
        df_full = pd.read_csv('..\data\saman_3471_STA-IS0052A.csv', sep=";")
    # Umbreyta gögnum eftir þörfum
    df_full['datetime'] = pd.to_datetime(df_full['endtime'], format='%Y-%m-%d %H:00:00')
    df_full['datetime'] = pd.DatetimeIndex(df_full['datetime'])
    df_full = df_full.set_index(['datetime'])
    df_full['datetime'] = df_full.index
    df_full = df_full.resample('60T').sum()
    df_full[efni].loc[df_full[efni] > 500] = np.nan
    df_full['ws'] = df_full['F (m/s)']
    df_full['wd'] = df_full['D (°)']
    df = df_full[['ws','wd',efni]]
    df[efni].loc[df[efni] == 0] = np.nan
    # Gera dálk með mæligildum hliðruðum um heila viku aftur í tíma
    df[prev] = df[efni].shift(+168)
    df[prev] = df[prev].fillna(method='bfill')
    df.tail()
    df = df.dropna()
    df['datetime'] = df.index
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

    # Endurraða, síðasti verður mark-dálkurinn, henda burt óþarfa gögnum
    df = df[['datetime','n_yd_x','n_yd_y',
            'n_wd_x','n_wd_y','n_hr_x','n_hr_y',
            'wd_x','wd_y','ws', prev, efni]]
    cols = ['n_yd_x','n_yd_y',
            'n_wd_x','n_wd_y','n_hr_x','n_hr_y',
            'wd_x','wd_y','ws', prev, efni]
    # Normalisera
    means = df[cols].mean(axis=0)
    stds = df[cols].std(axis=0)
    df[cols] = df[cols] .apply(lambda x: (x - x.mean()) / x.std())

    # Skipta gagnasetti upp í train, validation og test set.
    # X_Train, y_Train, X_Valid, y_Valid, X_Test, y_Test, mean, std, AllData, AllDataWithForecast
    if stod == 'Reykjavik':
        t1 = '2017-03-01'
        t2 = '2019-01-01'
    if stod == 'Akureyri':
        # Akureyrarstöðin hefur styttri samfellda tímaröð
        t1 = '2019-06-01'
        t2 = '2020-02-01'

    X_DATA = df
    mask = (df['datetime'] < t1)
    X_Train = df.loc[mask].drop(columns=['datetime',efni])
    y_Train = df.loc[mask,efni]#.drop(columns=['datetime'])
    #
    mask = (df['datetime'] < t2) & (df['datetime'] >= t1)
    X_Valid = df.loc[mask].drop(columns=['datetime',efni])
    y_Valid = df.loc[mask,efni]#.drop(columns=['datetime'])
    #
    mask = (df['datetime'] >= t2)
    X_Test = df.loc[mask].drop(columns=['datetime',efni])
    y_Test = df.loc[mask,efni]#.drop(columns=['datetime'])

    def print_scores(m, X_train=X_Train, X_valid=X_Valid, y_train = y_Train, y_valid = y_Valid):
        preds = m.predict(X_valid, 10000).flatten()
        print('Train MSE = ', mse(y_train, m.predict(X_train, 10000).flatten()),
                ', Valid MSE = ', mse(y_valid, preds))

    def print_scores_Tree(m, X_train=X_Train, X_valid=X_Valid, y_train=y_Train, y_valid=y_Valid):
        print('Train MSE = ', mse(y_train, m.predict(X_train)),
                ', Valid MSE = ', mse(y_valid, m.predict(X_valid)))

    def mse(y_true, y_pred):
        return ((y_true - y_pred)**2).mean()

    # Stilla upp neural network líkani
    nn = Sequential([Dense(256, input_shape=(X_Train.shape[1],), activation="relu"), Dense(1, activation="linear")])
    nn.summary()
    nn.compile(Adam(1e-1), "mse")
    # Þjálfa
    history = nn.fit(X_Train, y_Train, 1024, epochs=40, validation_data=(X_Valid, y_Valid))#,sample_weight=y_Train**2)#abs(y_Train+0.5))

    print_scores(nn)

    if plot_diagnostics:
        #Plotta MSE vs epochs
        loss_vals = history.history['loss']
        epochs = range(1, len(loss_vals)+1)
        plt.plot(epochs, loss_vals, label='Training Loss')
        plt.xlabel('Epochs')
        plt.ylabel('Mean Square Error')
        plt.legend()
        plt.title('MSE vs Epochs')

        tmp = pd.DataFrame(nn.predict(df.drop(columns=['datetime',efni])).flatten(), columns=["NN_"+efni])
        tmp.index = df.index
        op = df.join(tmp)
        mask = (op.datetime < '2019-10-15') & (op.datetime > '2019-10-01')

        plt.figure()
        ax=plt.gca()
        plt.plot(op.datetime.loc[mask],op["NN_"+efni].loc[mask]*stds[efni]+means[efni],'m-',linewidth=0.5)
        plt.plot(op.datetime.loc[mask],op[efni].loc[mask]*stds[efni]+means[efni],'k-',linewidth=1)

    # Vista líkan og meðaltöl/staðalfrávik
    nn.save('..\\models\\likan_'+stod+'_'+efni)
    means.to_pickle('..\\models\\means_'+stod+'_'+efni)
    stds.to_pickle('..\\models\\stds_'+stod+'_'+efni)
