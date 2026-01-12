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


os.makedirs('/app/data/scrapper_vol/mercados/' , exist_ok=True)
# %%

nuevo_link='https://www.dia.es/api/v1/pdp-insight/initial_analytics/166802'
nueva_pagina=urllib.request.Request(nuevo_link)
nueva_pagina.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0')
nueva_pagina.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8')
nueva_pagina.add_header('Accept-Language', 'en-US,en;q=0.5')
web = urllib.request.urlopen(nueva_pagina).read()
doc_nuevo=BeautifulSoup(web,'lxml')
test=json.loads(doc_nuevo.text)
enlace=[]

try:
    for i in test["menu_analytics"]:
        for j_ind,j in enumerate(test["menu_analytics"][i]["children"]):
            if j_ind!=0:
                print(test["menu_analytics"][i]["children"][j]["path"])
                enlace.append(test["menu_analytics"][i]["children"][j]["path"])
except KeyError:
    print("Key not found")
# %% [markdown]
# enlace=conseguir_categorias()

# %%
for k_idx,k in enumerate(enlace):
   

    
    nombre=[]
    categoria=[]
    subcategoria=[]
    peso=[]
    precio=[]
    precio_unidad=[]
    unidades=[]
    comercio=[]
    fecha=[]
    try:
        url="https://www.dia.es/api/v1/plp-back/reduced"+enlace[k_idx]
        print(url)
        #pag=urllib.request.urlopen(url)
        #doc=BeautifulSoup(pag)
        #resp = requests.get(url=url)
        #data=resp.json()
        
        
        pag=urllib.request.Request(url)
        pag.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0')
        pag.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8')
        pag.add_header('Accept-Language', 'en-US,en;q=0.5')
        web = urllib.request.urlopen(pag).read()
        doc_nuevo=BeautifulSoup(web,'lxml')
        data=json.loads(doc_nuevo.text)
        
        
        for i in range(len(data["plp_items"])):
            nombre.append(data["plp_items"][i]["display_name"])
            categoria.append(url.split("/")[7])
            subcategoria.append(url.split("/")[8])
            peso.append(data["plp_items"][i]["weight"])
            precio.append(data["plp_items"][i]["prices"]["price"])
            precio_unidad.append(data["plp_items"][i]["prices"]["price_per_unit"])
            unidades=data["plp_items"][i]["prices"]["measure_unit"]
            comercio.append("dia")
            fecha.append(str(datetime.today()).split(" ")[0])
    except:
        print("error",enlace[k_idx])
        pass

    
    pag_nav=2
    while True:
        try:
            if len(data["plp_items"])>=20:
                new_url="https://www.dia.es/api/v1/plp-back/reduced"+"/".join(enlace[k_idx].split("/")[:-2])+f"/pag-{pag_nav}/"+"/".join(enlace[k_idx].split("/")[-2:])
                
                #pag=urllib.request.urlopen(new_url)
                #doc=BeautifulSoup(pag)
                #resp = requests.get(url=new_url)
                #data=resp.json()
                print(new_url)
                
                pag=urllib.request.Request(new_url)
                pag.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0')
                pag.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8')
                pag.add_header('Accept-Language', 'en-US,en;q=0.5')
                web = urllib.request.urlopen(pag).read()
                doc_nuevo=BeautifulSoup(web,'html.parser')
                data=json.loads(doc_nuevo.text)

                for i in range(len(data["plp_items"])):
                    nombre.append(data["plp_items"][i]["display_name"])
                    categoria.append(url.split("/")[7])
                    subcategoria.append(url.split("/")[8])
                    peso.append(data["plp_items"][i]["weight"])
                    precio.append(data["plp_items"][i]["prices"]["price"])
                    precio_unidad.append(data["plp_items"][i]["prices"]["price_per_unit"])
                    unidades=data["plp_items"][i]["prices"]["measure_unit"]
                    comercio.append("Dia")
                    fecha.append(str(datetime.today()).split(" ")[0])
                    
                pag_nav=pag_nav+1
            else:
                break
        except:
            break
        
    
    new_df=pd.DataFrame(data={
             "nombre":nombre,
             "categoria":categoria,
             "subcategoria":subcategoria,
             "peso":peso,
             "precio":precio,
             "precio_unidad":precio_unidad,
             "unidades":unidades,
             "comercio":comercio,
             "fecha":fecha           

        })
        
    if os.path.isfile("/app/data/scrapper_vol/mercados/dia.csv")==True:
        datos=pd.read_csv("/app/data/scrapper_vol/mercados/dia.csv",sep="|",index_col=False)
        final_df=pd.concat([datos,new_df],axis=0)
        final_df.to_csv("/app/data/scrapper_vol/mercados/dia.csv",sep="|",index=False,encoding="utf-8")
    else:
            new_df.to_csv("/app/data/scrapper_vol/mercados/dia.csv",sep="|",index=False,encoding="utf-8") 




