### api_gogn.py
# Hofundar: Svifrykssveinar
# Dags: 14.8.2020
# Sidast breytt: 17.8.2020 
###
# Inniheldur foll sem na i gogn i gegnum opinber API

import csv
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime

# from keras.models import Sequential
# from keras.layers import *
# from keras.optimizers import SGD, Adam
# import keras
# import tensorflow as tf
# import keras.backend as kb


def pull_ust_api():
    #Fall sem sækir nýjustu gögn frá ust, fletur út json strenginn og vistar sem .csv skrá í /data/ust_latest.csv

    #Hreinsa út skjalið og skrifa nöfn á dálkum
    with open('data/ust_latest.csv','w', newline='') as csv_file:
        header = csv.writer(csv_file)
        header.writerow(['station_name', 'pollutantnotation','endtime','the_value','resolution','verification','station_local_id','concentration'])

    #Vefsíða Umhverfisstofnunar með nýjust mæligildum
    url = "https://api.ust.is/aq/a/getLatest"

    #Ná í gögn gegnum API
    response = requests.get(url)

    #Ef code != 200 er villa 
    if response.status_code == 200:
        #Breyta gögnum í json form
        ust_json = response.json()

        #Fara í gegnum allan json strenginn
        for station in ust_json:
            #print("NY STOD")
            #print(station)
            
            nafn = ust_json[station]['name']
            loc_id = ust_json[station]['local_id']
            #print('nafn', nafn)
            #print('local_id', loc_id)
            for parameter in ust_json[station]['parameters']:
                #print("NYR PARAM")
                #print(parameter)
                
                res = ust_json[station]['parameters'][parameter]['resolution']
                uni = ust_json[station]['parameters'][parameter]['unit']
                #print("res", res)
                #print('uni', uni)
                for obs in ust_json[station]['parameters'][parameter]:
                    if obs not in ('resolution', 'unit'):
                        #print("NYTT OBS")
                        #print(obs)
                        #print(ust_json[station]['parameters'][parameter][obs])

                        with open('data/ust_latest.csv','a', newline='') as csv_file:
                            header = csv.writer(csv_file)
                            header.writerow([nafn, parameter,ust_json[station]['parameters'][parameter][obs]['endtime'], \
                                            ust_json[station]['parameters'][parameter][obs]['value'], \
                                            res,
                                            ust_json[station]['parameters'][parameter][obs]['verification'], \
                                            station, uni])

        #Loka .csv skrá eftirá
        csv_file.close()

def pull_vedur_brunnur(dags = '', stodvar = ['1475'], initiate = 'F'):
    #Fall sem sækir veðurathugunargöng frá brunnur.vedur.is

    url = "http://brunnur.vedur.is/athuganir/athtafla/" + dags + ".html"
    response = requests.get(url)

    if response.status_code == 200:
        #Notum BeautifoulSoup til að lesa úr html gögnunum
        brunnur_soup = BeautifulSoup(response.content, 'html.parser')
        count = 0
        for row in brunnur_soup.find_all('tr'):
            #Fara í gegnum hverja röð í töflunni
            if count >= 0:
                row_list = list()
                row_list.append(dags)
                #print(row)
                for i in row:
                    if i != '\n':
                        row_list.append(i.string)
                #print(row_list)   
                if initiate == 'T':
                    #Prenta header línu efst ef þetta er fyrsta beiðni
                    if row_list[1] == 'Stöð':
                        for stod in stodvar:
                            with open('data/brunnur_gamalt_' +stod+ '_2015_2020.csv','w', newline='') as csv_file:
                                    header = csv.writer(csv_file)
                                    header.writerow(row_list)
                            #Loka .csv skrá eftirá
                            csv_file.close()
                else:
                    #Annars bæta bara einni línu í einu skjalið    
                    if row_list[1] in stodvar:
                        with open('data/brunnur_gamalt_' +str(row_list[1])+ '_2015_2020.csv','a', newline='') as csv_file:
                                    header = csv.writer(csv_file)
                                    header.writerow(row_list)
                        #Loka .csv skrá eftirá
                        csv_file.close()
                        
            count += 1    

def pull_vedur_gamalt(fra_dags, til_dags, stodvar, nytt = True):
    #Fall sem býr til .csv skrá fyrir veðurathuganir á tímabili fyrir lista af stöðvum.
    #fra_dags skal vera hærri en til_dags
    #fra_dags og til_dags skulu vera int á forminu YYYYMMDDHH, dæmi 2020081623
    #Fallið notar pull_vedur_brunnur()
    
    dags = fra_dags
    if nytt == False:
        print("gera frá upphafi")
        #pull_vedur_brunnur(dags= str(dags), stodvar = stodvar, initiate= 'T')
        #print('buid að initiatea')

    count = 0

    while dags > til_dags:
        #Fara í gegnum allar dagsetningar, nýjasta fyrst
        if dags % 100 > 23:
            dags = 100 * (dags // 100) + 23
        if (dags % 10000) // 100 == 0:
            #ef næsti mán er með 31 degi
            if (dags % 1000000) // 10000 in (2, 4, 6, 8, 9, 11):
                dags = 10000*(dags // 10000 - 1) + 3100 + 23
            #ef næsti mán ef með 30 daga
            elif (dags % 1000000) // 10000 in (5, 7, 10, 12):
                dags = 10000*(dags // 10000 - 1) + 3000 + 23
            #ef næsti mán ef nýtt ár
            elif (dags % 1000000) // 10000 == 1:
                dags = 1000000*(dags // 1000000 - 1) + 120000 + 3100 + 23
            #ef næsti mán ef febrúar
            elif (dags % 1000000) // 10000 == 3:
                if (dags // 1000000) % 100 == 0:
                    dags = 10000*(dags // 10000 - 1) + 2800 + 23
                elif (dags // 1000000) % 4 == 0:
                    dags = 10000*(dags // 10000 - 1) + 2900 + 23
                else:
                    dags = 10000*(dags // 10000 - 1) + 2800 + 23
                
        print('dags ', str(dags))
        pull_vedur_brunnur(dags= str(dags), stodvar = stodvar, initiate= 'F')
        time.sleep(1)
        count += 1
        dags -= 1
    
def pull_vedurspa(stodvar = ['1','422']):
    #Fall sem nær í nýjustu veðurspá frá XML þjónustu vedur.is 

    #Vefsíða Umhverfisstofnunar með nýjust mæligildum
    url = "https://xmlweather.vedur.is/"
    
    stod_count = 0
    for stod in stodvar:
        #leiðbeiningar fyrir param' https://www.vedur.is/media/vedurstofan/XMLthjonusta.pdf
        param = "?op_w=xml&type=forec&lang=is&view=csv&ids="+str(stod)+"&params=R;T;FX;D;W;N;P;RH;TD"
        #Ná í gögn gegnum API
        print(url + param)
        response = requests.get(url + param)
        
        with requests.Session() as s:
            download = s.get(url+param)

            decode_data = download.content.decode('utf-8')

            cr = csv.reader(decode_data.splitlines(), delimiter = ',')

            my_list = list(cr)
            count = 0
            for row in my_list:
                if count == 0 and stod_count == 0:
                    #Skrifum header bara frá fyrstu stöð
                    with open('data/vedurspa.csv','w', newline='') as csv_file:
                        header = csv.writer(csv_file)
                        header.writerow(row)
                elif count > 0:
                    #Sleppum header fyrir seinni stöðvar
                    with open('data/vedurspa.csv','a', newline='') as csv_file:
                        header = csv.writer(csv_file)
                        header.writerow(row)
                count += 1
        stod_count += 1
        
def get_ust_csv():
    #Fall sem sameinar 2015-2019 gögn frá api.ust.is með 2020 gögnum frá loftgæði.is
    #Fallið skilar pandas dataframe

    #Sækja 2015-2019 gögn sem hlaðið var niður frá api.ust.is
    ust_csv = pd.read_csv('data/ust_aq_timeseries_2015.csv', encoding = 'utf-8')
    for i in list(range(2016,2019 + 1)):
        ust_csv = ust_csv.append(pd.read_csv('data/ust_aq_timeseries_'+str(i)+'.csv', encoding = 'utf-8')) 
    ust_csv['endtime'] = pd.to_datetime(ust_csv['endtime'], format='%Y-%m-%d %H:00:00')
    
    #Sækja 2020 gögn sem var hlaðið niður frá loftgæði.is (auk nýjustu gilda frá api)
    ust_2020 = pd.read_csv('data/ust_2020_formatted.csv',  encoding = "ISO-8859-1", sep=';')
    ust_2020['endtime'] = pd.to_datetime(ust_2020['endtime'], format='%Y-%m-%d %H:00:00')

    #Hreinsa nafn á resolution dálki
    # cols_2020 = list(ust_2020)
    # del ust_2020[str(cols_2020[0])]
    ust_2020['resolution'] = '1h'

    sameindad = pd.concat([ust_csv, ust_2020])   
    return sameindad   

def vista_latest_brunnur(stodvar = ['1475']):
    #Fall sem vistar ný gögn frá grunnur inn í skjal með fyrri gögnum
    #stodvar skal vera listi af strengjum sem eru veðurstöðvarnúmer

    timi_fra = int(datetime.utcnow().strftime('%Y%m%d%H')) 

    for stod in stodvar: 
        #Finna nýjasta skráða tímann
        brunnur_csv = pd.read_csv('data/brunnur_gamalt_' +str(stod)+ '_2015_2020.csv', encoding = "ISO-8859-1")
        # print(brunnur_csv.head())
        # print(brunnur_csv.tail())

        timi_til = brunnur_csv['dags'].max()

        # print(timi_til)
        pull_vedur_gamalt(fra_dags = timi_fra, til_dags = timi_til, stodvar = [stod], nytt = True)

        brunnur_csv = pd.read_csv('data/brunnur_gamalt_' +str(stod)+ '_2015_2020.csv', encoding = "ISO-8859-1")
        # print(brunnur_csv.head())
        # print(brunnur_csv.tail())


def vista_latest_ust():
    #Fall sem sameinar 2020 gögnin frá loftgæði.is með nýjustu api.ust.is gögnin í sömu skrá 

    #Sækja 2020 gögn sem var hlaðið niður frá loftgæði.is (auk nýjustu gilda frá api)
    ust_2020 = pd.read_csv('data/ust_2020_formatted.csv',  encoding = "ISO-8859-1", sep=';')
    ust_2020['endtime'] = pd.to_datetime(ust_2020['endtime'], format='%Y-%m-%d %H:00:00')
    
    #Hreinsa nafn á resolution dálki
    # cols_2020 = list(ust_2020)
    # del ust_2020[str(cols_2020[0])]
    ust_2020['resolution'] = '1h'

    latest_ust = pd.read_csv('data/ust_latest.csv',  encoding = "ISO-8859-1", sep = ',')
    latest_ust['endtime'] = pd.to_datetime(latest_ust['endtime'], format='%Y-%m-%d %H:00:00')
    # print(latest_ust.head())
    # print(ust_2020.tail())
    latest_ust['the_value'] = latest_ust.the_value.apply(str)
    latest_ust['the_value'] = latest_ust[['the_value']].apply(lambda x: x.str.replace('.',','))
    # latest_ust['the_value'] = latest_ust.the_value.apply(float)

    # print(latest_ust.station_local_id.unique())
    # print(type(latest_ust.station_local_id.unique()))
    # print(pd.DataFrame(data=latest_ust.station_local_id.unique(), columns=["station_local_id"]))
    latest_stodvar = pd.DataFrame(data=latest_ust.station_local_id.unique(), columns=["station_local_id"])
    print(ust_2020)
    stodvar_2020 = pd.DataFrame(data=ust_2020.station_local_id.unique(), columns=["station_local_id"])
    # print(latest_stodvar)
    # print(type(latest_stodvar))
    # print(stodvar_2020)
    # print(type(stodvar_2020))
    
    stodvar = latest_stodvar.join(stodvar_2020.set_index('station_local_id'), on='station_local_id', how = 'inner')
    # print('stodvar')
    # print(stodvar)

    count = 0
    for index, stod in stodvar.iterrows():
        # print('inni i for')
        # print(type(stod), str(stod[0]))
        # print(type(index), index)
        
        if 'STA-' in str(stod):
            #print(type(stod), stod)
            # print("inni i if")
            latest_stod = latest_ust.loc[(latest_ust['station_local_id'] == str(stod[0]))]
            ust_2020_stod = ust_2020.loc[(ust_2020['station_local_id'] == str(stod[0]))]
            
            # print(latest_stod.head())
            # print(latest_stod.tail())
            # print(ust_2020_stod.head())
            # print(ust_2020_stod.tail())

            latest_stod_efni = pd.DataFrame(data=latest_stod.pollutantnotation.unique(), columns=["pollutantnotation"])
            stodvar_2020_efni = pd.DataFrame(data=ust_2020_stod.pollutantnotation.unique(), columns=["pollutantnotation"])
            # print(latest_stod_efni)
            # print(type(latest_stod_efni))
            # print(stodvar_2020_efni)
            # print(type(stodvar_2020_efni))
            
            efnin = latest_stod_efni.join(stodvar_2020_efni.set_index('pollutantnotation'), on='pollutantnotation', how = 'inner')

            # latest_efni = latest_stod.pollutantnotation.unique()
            # ust_2020_efni = ust_2020_stod.pollutantnotation.unique()
            # efni = latest_efni.set_index('pollutantnotation').join(ust_2020_efni.set_index('pollutantnotation'), how = 'inner')
            for index, efni in efnin.iterrows():
                
                # print('inni i efni for loop')
                # print(efni)
                
                ust_2020_stod_efni = ust_2020_stod.loc[ust_2020['pollutantnotation'] == str(efni[0])]
                latest_stod_efni = latest_stod.loc[latest_stod['pollutantnotation'] == str(efni[0])]

                most_recent = ust_2020_stod_efni['endtime'].max()
                # print(most_recent)
                # print(type(most_recent))
                
                if count == 0:
                    latest_stod_efni_nytt = latest_stod_efni.loc[latest_stod_efni['endtime'] > most_recent]
                else:
                    latest_stod_efni_nytt = latest_stod_efni_nytt.append(latest_stod_efni.loc[latest_stod_efni['endtime'] > most_recent])
                print(latest_stod_efni_nytt.head())
                print(latest_stod_efni_nytt.tail())
                
                #Vista nýju gildin í 2020 skrána
                # for latest_index, latest_row in latest_stod_efni_nytt.iterrows():
                #     with open('data/ust_2020_formatted_test.csv','a', newline='', encoding = "ISO-8859-1") as csv_file:
                #         header = csv.writer(csv_file)
                #         header.writerow([latest_row['resolution'], latest_row['station_local_id'], latest_row['station_name'], latest_row['endtime'], \
                #                         latest_row['the_value'], latest_row['pollutantnotation'], \
                #                         latest_row['concentration']])
                count += 1
    print('SIDASDA PRENT')
    print(ust_2020.head())
    print(ust_2020.tail())
    print(ust_2020.append(latest_stod_efni_nytt).head())
    print(ust_2020.append(latest_stod_efni_nytt).tail())
    ust_2020.append(latest_stod_efni_nytt).to_csv("data/ust_2020_formatted.csv",sep = ';', index = False)
    
    


def sameina_ust_vedur(vedur_stod = 1475, ust_stod = 'STA-IS0005A'):
    #Fall sem sameinar veðurgögn og ust gögn fyrir eina veðurstöð og eina ust stöð í eina skrá
    #Fallið skilar staðsetningu skráarinnar

    #Lesa inn veðurgögn sem búið er að vista
    vedur_csv = pd.read_csv('data/brunnur_gamalt_'+str(vedur_stod)+'_2015_2020.csv', encoding = "ISO-8859-1")
    vedur_csv['endtime'] = pd.to_datetime(vedur_csv['dags'], format='%Y%m%d%H')
    vedur_stod_csv = vedur_csv[vedur_csv['Stöð'] == vedur_stod] 
    # print(vedur_stod_csv.head())
    # print(vedur_stod_csv.tail())

    #Ná í ust gögn
    ust_csv = get_ust_csv()
    ust_stod_csv = ust_csv[ust_csv['station_local_id'] == ust_stod] 
    # print(ust_stod_csv.head())
    # print(ust_stod_csv.tail())
    
    #Breyta kommutölum í punkttölur
    ust_stod_csv['the_value'] = ust_stod_csv.the_value.apply(str)
    ust_stod_csv['the_value'] = ust_stod_csv[['the_value']].apply(lambda x: x.str.replace(',','.'))
    ust_stod_csv['the_value'] = ust_stod_csv.the_value.apply(float)
    #print((ust_stod_csv[['the_value']]))
    #print(ust_stod_csv.the_value.apply(str))
    #print(type(ust_stod_csv[['the_value']]))

    #Fletja út ust töfluna
    ust_stod1 = ust_stod_csv[['station_name','station_local_id','endtime','pollutantnotation','the_value']].pivot_table(index=['station_name','station_local_id','endtime'], columns='pollutantnotation')
    # print(ust_stod1.head())
    # print(ust_stod1.tail())
    ust_stod1.columns = ust_stod1.columns.droplevel().rename(None)
    # print(ust_stod1.head())
    # print(ust_stod1.tail())
    
    #Vista flötu töfluna sem .csv til að fá fram allar breytur
    ust_stod1.reset_index().fillna("null").to_csv("data/temp.csv", sep=";", index=None)
    ust_stod_flat = pd.read_csv('data/temp.csv', encoding = "ISO-8859-1", sep = ';')
    # print(ust_stod_flat.head())
    # print(ust_stod_flat.tail())

    #Joina saman veður og ust gögn á tímadálkinum
    saman = vedur_stod_csv.set_index('endtime').join(ust_stod_flat.set_index('endtime'), how = 'outer')
    # print(saman.head())
    # print(saman.tail())

    #Vista skrá
    skra = "data/saman_"+str(vedur_stod)+"_"+str(ust_stod)+".csv"
    saman.reset_index().to_csv(skra, sep=";", index=None)
    return skra

def uppfaera_allt(stadir_list = [{"stadur":"Reykjavik", "stodvar":{"vedur": '1475', "ust":"STA-IS0005A"}},{"stadur":"Akureyri", "stodvar":{"vedur": '3471', "ust":"STA-IS0052A"}}]):
    #Fall sem uppfærir veðurathugunargögn, veðurspá og loftgæðigögn
    #Fallið tekur inn lista af dict.
    #Í dict eru tveir lyklar, stadur, sem er lýsandi og ekki notaður. Hinn er stodvar sem inniheldur dict
    #Í stodvar dict eru tveir lyklar, einn vedur, hinn ust. Gildi þessara lykla skulu vera stöðvanúmer sem strengir

    #Uppfæra ust gögn
    pull_ust_api()
    print("Ný ust gögn komin")

    #Vista ust gogn saman við eldri
    vista_latest_ust()
    print("Ný ust gögn vistuð með 2020")

    #Ná í nýjustu veðurspá
    pull_vedurspa()
    print("Ný veðurspá vistuð")

    for stadir in stadir_list:
        print("Uppfærsla hefst fyrir:", stadir['stadur'])
        
        #Uppfæra vedurathugunargögn
        vista_latest_brunnur(stodvar= [stadir['stodvar']['vedur']])
        print("Brunnur uppfærður fyrir:", stadir['stadur'])

        #Sameina og vista nytt ust og vedur
        skra_stadsetning = sameina_ust_vedur(vedur_stod = int(stadir['stodvar']['vedur']), ust_stod = stadir['stodvar']['ust'])
        print(stadir['stadur'] + " hefur verið uppfært. Ný skrá má fina hér: " + skra_stadsetning)









print("byrja")
uppfaera_allt()
#pull_vedurspa()

#pull_ust_api()
#sameina_ust_vedur()
#sameina_ust_vedur(vedur_stod=3471, ust_stod='STA-IS0052A')
#vista_latest_brunnur(stodvar= ['1475', '3471'])
# vista_latest_ust()
#sameina_ust_vedur()


    
