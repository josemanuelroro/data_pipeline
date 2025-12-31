import os
import time
import requests
import datetime


TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


LOG_FILE = "/infraestructure/data_shared/logs/serv_access.log"

def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        print(f"Enviando alerta: {mensaje}")
        requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje})
    except Exception as e:
        print(f"Error enviando Telegram: {e}")


def follow(thefile):

    thefile.seek(0, os.SEEK_END)
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(0.5) 
            continue
        yield line


if __name__ == "__main__":
    print(f"---  Vigilante de Jenkins (Tiempo Real) Iniciado: {datetime.datetime.now()} ---")
    

    while not os.path.exists(LOG_FILE):
        print(f"Esperando fichero: {LOG_FILE}...")
        time.sleep(5)

    print("Fichero detectado. Monitorizando...")


    try:
        with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as logfile:
            
            
            logs_en_tiempo_real = follow(logfile)
            
            for line in logs_en_tiempo_real:
                if line:
                    parts = line.split(' ')
                    
                    if len(parts) >= 3:
                        fecha = parts[3].strip()
                        ip = parts[0].strip()
                        tipo=parts[8].strip()
                        
                        ip_info = ip if ip != "System/Internal" else "Interna/Docker"
                        
                        alert = f"⚡ Acceso Detectado \n IP: {ip_info} \n Fecha: {fecha} \n Tipo: {tipo}"
                        enviar_telegram(alert)

    except KeyboardInterrupt:
        print("\n Deteniendo Watcher...")
    except Exception as e:
        print(f"💀 Error Fatal: {e}")




