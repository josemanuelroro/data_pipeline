import os
import time
import requests
import datetime
from collections import deque

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        print(f"Enviando alerta: {mensaje}")
        requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje})
    except Exception as e:
        print(f"Error enviando Telegram: {e}")

salida=[]
with open("/backend/data_shared/logs/mercados.log", 'r', encoding='utf-8', errors='ignore') as logfile:
    for line in deque(logfile, maxlen=3):
        
        print(line.strip())
        salida.append(line.strip())
    enviar_telegram(f"📅{salida[0].split('|')[4]}\n🏪 {(salida[0].split('|')[0]).upper()} Inserted: {salida[0].split('|')[2]}\
        \n🏪 {(salida[1].split('|')[0]).upper()} Inserted: {salida[1].split('|')[2]}\
        \n🏪 {(salida[2].split('|')[0]).upper()} Inserted: {salida[2].split('|')[2]}")