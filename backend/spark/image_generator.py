import time
import os
import pandas as pd
import matplotlib
import matplotlib.cm as cm
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import MaxNLocator

CARPETA_COMPARTIDA = "/app/data/scrapper_vol/requests/"
ARCHIVO_TRIGGER = os.path.join(CARPETA_COMPARTIDA, "request.flag")
ARCHIVO_IMAGEN = os.path.join(CARPETA_COMPARTIDA, "image.png")

def image():  
    df=pd.read_parquet("s3://gold/acceso_logs", storage_options=MINIO_OPTS,engine='pyarrow')
    fig, ax=plt.subplots(nrows=2, ncols=2, figsize=(12, 8))

    ####number of connections by country
    num_conexiones_paises=df.groupby("country",observed=True).count()
    num_conexiones_paises=num_conexiones_paises.sort_values(by="ip",ascending=False)
    df_city_country=df.groupby(['country', 'city'],observed=True)['ip'].count().reset_index()
    df_city_country=df_city_country.sort_values('ip', ascending=False).head(10)
    todos_los_paises=num_conexiones_paises.index.unique()
    cmap=plt.get_cmap('tab10')
    lista_colores = [cmap(i) for i in range(len(todos_los_paises))]
    diccionario_colores=dict(zip(todos_los_paises, lista_colores))
    colores_para_grafico_1=[diccionario_colores[pais] for pais in num_conexiones_paises.index]
    colores_para_grafico_2=df_city_country['country'].map(diccionario_colores)



    mis_barras=ax[0,0].bar(num_conexiones_paises.index,num_conexiones_paises["ip"], color=colores_para_grafico_1)
    for barra, nombre_pais in zip(mis_barras, num_conexiones_paises.index):
        ax[0,0].text(
            barra.get_x() + barra.get_width() / 2, 
            0.5,                                     
            nombre_pais,                           
            ha='center',                           
            va='bottom',                          
            rotation=90,                           
            color='black'                        
        )
    ax[0,0].set_title("Connections by Country")
    ax[0,0].tick_params(axis='x', bottom=True, labelbottom=False)
    ax[0,0].set_ylabel("Connections")
    ax[0,0].yaxis.set_major_locator(MaxNLocator(integer=True))

    ####number of connections by city
    mis_barras_ciudades=ax[1, 0].bar(df_city_country['city'],df_city_country['ip'], color=colores_para_grafico_2)
    for barra, nombre_ciudad in zip(mis_barras_ciudades, df_city_country['city']):
        ax[1,0].text(
            barra.get_x() + barra.get_width() / 2, 
            0.2,                                
            nombre_ciudad,
            ha='center', 
            va='bottom',
            rotation=90,
            color='black', 
            fontsize=9
        )

    ax[1,0].set_title("Cities")
    ax[1,0].tick_params(axis='x', bottom=False, labelbottom=False)
    ax[1,0].set_ylabel("Connections")
    paises_en_grafico_ciudades = df_city_country['country'].unique()
    #handles=[mpatches.Patch(color=diccionario_colores[p], label=p) for p in paises_en_grafico_ciudades]
    #ax[1,0].legend(handles=handles, title="Country", fontsize='small')
    ax[1,0].yaxis.set_major_locator(MaxNLocator(integer=True))

    ####number of connections by isp
    num_conexiones_isp=df.groupby("isp",observed=True).count()
    num_conexiones_isp=num_conexiones_isp.sort_values(by="ip",ascending=False)
    mis_barras=ax[0,1].bar(
        num_conexiones_paises.index,
        num_conexiones_paises["ip"], 
        color=[cmap(i) for i in range(len(num_conexiones_paises))]
        )
    for barra, nombre_pais in zip(mis_barras, num_conexiones_isp.index):
        ax[0,1].text(
            barra.get_x() + barra.get_width() / 2, 
            0.5,
            nombre_pais,
            ha='center',
            va='bottom',
            rotation=90,                           
            color='black')        
    ax[0,1].set_ylabel("Connections")
    ax[0,1].set_title("Connections by Isp")
    ax[0,1].tick_params(axis='x', bottom=True, labelbottom=False)
    ax[0,1].yaxis.set_major_locator(MaxNLocator(integer=True))

    ####number of connections by days
    cmap=plt.get_cmap('tab20')
    num_conexiones_days=df.groupby("fecha",observed=True).count()
    ax[1,1].plot(num_conexiones_days.index,num_conexiones_days["ip"])
    ax[1,1].set_title("days")
    ax[1,1].set_ylabel("Connections")
    ax[1,1].set_xlabel("Date")


    plt.savefig(ARCHIVO_IMAGEN)
    plt.close()
    return

MINIO_OPTS = {
    "key": os.getenv("MINIO_USER"),
    "secret": os.getenv("MINIO_PASSWORD"),
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}
while True:
    
    if os.path.exists(ARCHIVO_TRIGGER):
        print("File Detected generating image")
        image()
        os.remove(ARCHIVO_TRIGGER)
    else:
        pass
    time.sleep(1)
 


