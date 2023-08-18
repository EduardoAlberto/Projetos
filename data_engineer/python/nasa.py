# teste api nasa
import json, requests, http.client
import pandas as pd
from datetime import datetime

class nasa:
    def neo():
        url = "https://api.nasa.gov/neo/rest/v1/neo/3542519?api_key=u6vJzHOIX3TVwiXkcSHTHlh4Im6rPDxabT0zWJFi"
        response = requests.get(url)
        carga =[]
        arq = response.json()
        carga.append(arq)
        df = pd.json_normalize(carga)
        df = pd.DataFrame(df["designation"])
        return df
    
    def earth():
        url = "https://api.nasa.gov/planetary/earth/assets?lon=-95.33&lat=29.78&date=2018-01-01&&dim=0.10&api_key=u6vJzHOIX3TVwiXkcSHTHlh4Im6rPDxabT0zWJFi"
        response = requests.get(url)
        carga = []
        arq = response.json()
        carga.append(arq)
        df = pd.json_normalize(carga)
        df = pd.DataFrame(df["date"].str.slice(0,10))
        return df


x = nasa
t = x.earth()
print(t)