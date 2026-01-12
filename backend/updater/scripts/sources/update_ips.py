#========================================================================
#The script connects to Maxmind web and dowload the .csv files
#------------------------------------------------------------------------
#2026/01/12 Add the extraction fo the city block ips
#
#
#
#
#
#========================================================================
import requests
import zipfile
import io
import os
import shutil


ID=os.getenv("MAXMIND_ID")
KEY=os.getenv("MAXMIND_KEY")

#URLS
urls = ["https://download.maxmind.com/geoip/databases/GeoLite2-ASN-CSV/download?suffix=zip",
       "https://download.maxmind.com/geoip/databases/GeoLite2-City-CSV/download?suffix=zip",
       "https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip"]
names = ["asn","city","country"]

for url,name in zip(urls,names):

    print(name)
    r= requests.get(url,auth=(ID, KEY),stream=True)
    #Checks the url of each file
    with open(f"/app/source_data/{name}.zip", 'wb') as f:
        f.write(r.content)

    #Download the files
    with zipfile.ZipFile(f"/app/source_data/{name}.zip", 'r') as z:
        #Read the files inside the .zip
        for file_info in z.infolist():
            #print(file_info.filename)
            nombre_limpio = os.path.basename(file_info.filename)
            print(nombre_limpio)
            #Extract the specific file
            if nombre_limpio in ["GeoLite2-City-Locations-en.csv", "GeoLite2-ASN-Blocks-IPv4.csv","GeoLite2-Country-Locations-en.csv", "GeoLite2-City-Blocks-IPv4.csv"]  and nombre_limpio.endswith(".csv"):
                
                print(f"   -> Extrayendo: {nombre_limpio}")
                
                #Save the file
                if nombre_limpio =="GeoLite2-City-Blocks-IPv4.csv":
                    ruta_final = os.path.join("/app/source_data/", f"city_ip.csv")
                    with z.open(file_info) as fuente, open(ruta_final, "wb") as destino:
                                shutil.copyfileobj(fuente, destino)
                else:
                    ruta_final = os.path.join("/app/source_data/", f"{name}.csv")
                    with z.open(file_info) as fuente, open(ruta_final, "wb") as destino:
                                shutil.copyfileobj(fuente, destino)

