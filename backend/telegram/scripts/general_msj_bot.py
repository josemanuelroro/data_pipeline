import os
import datetime
import requests
import sys


TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        print(f"Enviando alerta: {mensaje}")
        requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje})
    except Exception as e:
        print(f"Error enviando Telegram: {e}")
mensaje=f"Lanzando tarea:\n{sys.argv[1]}\n{datetime.datetime.now()}"
enviar_telegram(mensaje)