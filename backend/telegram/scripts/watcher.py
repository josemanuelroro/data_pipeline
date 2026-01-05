import os
import time
import requests
import datetime

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
LOG_FILE = "/infraestructure/data_shared/logs/serv_access.log"

EXTENSIONES_IGNORAR = ('.css', '.js', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.svg', '.woff', '.woff2', '.map', '.ttf')
CACHE_IPS = {}
MI_IP = "172.24.0.1"
TIEMPO_SILENCIO = 600

def get_ip_details(ip):

    try:
        url = f"http://ip-api.com/json/{ip}?fields=status,countryCode,isp,org"
        response = requests.get(url, timeout=3).json()
        print(response)
        if response['status'] == 'success':
            return {
                'country_code': response['countryCode'],
                'owner': response['isp']
            }
    except Exception as e:
        print(f"Error geolocalizando IP: {e}")
    
    return {'country_code': 'UN', 'owner': 'Unkown'}


def get_flag_emoji(country_code):

    if not country_code or country_code == 'UN':
        return "🏴‍☠️"
        
    return chr(ord(country_code[0]) + 127397) + chr(ord(country_code[1]) + 127397)

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
    print(f"--- Vigilante de Jenkins (Tiempo Real) Iniciado: {datetime.datetime.now()} ---")

    while not os.path.exists(LOG_FILE):
        print(f"Esperando fichero: {LOG_FILE}...")
        time.sleep(5)

    print("Fichero detectado. Monitorizando...")

    try:
        with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as logfile:
            logs_en_tiempo_real = follow(logfile)
            
            for line in logs_en_tiempo_real:
                if line:
                    try:
                        parts = line.split(' ')
                        
                        if len(parts) >= 9:
                            ip = parts[0].strip()
                            fecha = parts[3].strip()
                            url = parts[6].strip()
                            tipo = parts[8].strip()

                            if any(url.endswith(ext) for ext in EXTENSIONES_IGNORAR):
                                continue

                            if ip == MI_IP:
                                continue

                            enviar_alerta = False

                            if tipo in ['401', '403', '404', '500', '502', '503']:
                                enviar_alerta = True
                            else:
                                tiempo_actual = time.time()
                                ultima_vez = CACHE_IPS.get(ip, 0)

                                if (tiempo_actual - ultima_vez) > TIEMPO_SILENCIO:
                                    enviar_alerta = True
                                    CACHE_IPS[ip] = tiempo_actual

                            if enviar_alerta:
                                ip_info = ip if ip != "System/Internal" else "Interna/Docker"
                                alert = f"⚡ Acceso Detectado \n IP: {ip_info} \n Fecha: {fecha} \n Tipo: {tipo} \n URL: {url}\n Pais: {get_ip_details(ip)['country_code']}{get_flag_emoji(get_ip_details(ip)['country_code'])} \n Org: {get_ip_details(ip)['owner']}"
                                enviar_telegram(alert)

                    except IndexError:
                        continue

    except KeyboardInterrupt:
        print("\n Deteniendo Watcher...")
    except Exception as e:
        print(f"💀 Error Fatal: {e}")
        