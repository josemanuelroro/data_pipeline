# %%
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
import json
import requests
import time
import warnings

# Ignorar solo los DeprecationWarning
warnings.filterwarnings("ignore")#, category=DeprecationWarning)
os.makedirs('/app/data/scrapper_vol/mercados/' , exist_ok=True)
# %%
def f_paginas():

    url='https://tienda.mercadona.es/api/categories/'
    pag=urllib.request.urlopen(url)
    doc=BeautifulSoup(pag,features='html.parser')
    resp = requests.get(url=url)
    data=resp.json()
    categorias=[]
    subcategoria=[]
    paginas=[]
    for i in range(len(data['results'])):
        for j in range(len(data['results'][i]['categories'])):
            #print(data['results'][i]['categories'][j]['id'])
            paginas.append(data['results'][i]['categories'][j]['id'])
            categorias.append(data['results'][i]["name"])
            subcategoria.append(data['results'][i]['categories'][j]['name'])
    return paginas,categorias,subcategoria
[num_paginas,categorias_lista,subcategorias_lista]=f_paginas()

# %%

for k_idx,k in enumerate(num_paginas):
        url='https://tienda.mercadona.es/api/categories/'+str(k)+'/'
        pag=urllib.request.urlopen(url)
        doc=BeautifulSoup(pag,features='html.parser')
        resp = requests.get(url=url)
        data=resp.json()
        nombre_l=[]
        iva_l=[]
        precio_l=[]
        unidades_l=[]
        bulk_precio_l=[]
        cantidad_l=[]
        unidades_l=[]
        categoria_l=[]
        subcategoria_l=[]
        tamano_l=[]
        peso_seco=[]
        granel=[]
        time.sleep(1)
        print(f"{k_idx+1}/{len(num_paginas)}",end=" ")
        for i in range(len (data["categories"])):

            for j in range(len(data["categories"][i]["products"])):

                nombre_l.append(data["categories"][i]["products"][j]["display_name"])
                iva_l.append(data["categories"][i]["products"][j]["price_instructions"]["iva"])
                precio_l.append(data["categories"][i]["products"][j]["price_instructions"]["unit_price"])
                tamano_l.append(data["categories"][i]["products"][j]["price_instructions"]["unit_size"])
                bulk_precio_l.append(data["categories"][i]["products"][j]["price_instructions"]["bulk_price"])
                cantidad_l.append(data["categories"][i]["products"][j]["price_instructions"]["total_units"])
                unidades_l.append(data["categories"][i]["products"][j]["price_instructions"]["reference_format"])
                categoria_l.append(categorias_lista[k_idx])
                subcategoria_l.append(subcategorias_lista[k_idx])
                peso_seco.append(data["categories"][i]["products"][j]["price_instructions"]["drained_weight"])
                granel.append(data["categories"][i]["products"][j]["packaging"])
        print(url)
        new_df=pd.DataFrame(data={
             "nombre":nombre_l,
             "categoria":categoria_l,
             "subcategoria":subcategoria_l,
             "tanaño":tamano_l,
             "precio":precio_l,
             "iva":iva_l,
             "cantidad":cantidad_l,
             "bulk_precio":bulk_precio_l,
             "unidades":unidades_l,
             "comercio":"Mercadona",
             "embalaje":granel,
             "peso__seco":peso_seco,
             "fecha":str(datetime.today()).split(" ")[0]

        })
        
        if os.path.isfile("/app/data/scrapper_vol/mercados/mercadona.csv")==True:
            datos=pd.read_csv("/app/data/scrapper_vol/mercados/mercadona.csv",sep="|",index_col=False)
            final_df=pd.concat([datos,new_df],axis=0)
            final_df.to_csv("/app/data/scrapper_vol/mercados/mercadona.csv",sep="|",index=False,encoding="utf-8")
        else:
             new_df.to_csv("/app/data/scrapper_vol/mercados/mercadona.csv",sep="|",index=False,encoding="utf-8")             


# %%


