### api_gogn.py
# Hofundar: Svifrykssveinar
# Dags: 14.8.2020
# Sidast breytt: 14.8.2020 
###
# Inniheldur foll sem na i gogn i gegnum opinber API
#

def pull_ust_api():
    #Fall sem sækir nýjustu gögn frá ust, fletur út json strenginn og vistar sem .csv skrá í /data/ust_latest.csv

    import requests
    import csv

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

    
