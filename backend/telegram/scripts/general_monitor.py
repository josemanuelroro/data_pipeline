import os
import time
import requests
import datetime
import logging
import subprocess
from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters,ApplicationBuilder

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = [int(os.getenv("TELEGRAM_CHAT_ID"))]

def is_allowed_chat(update: Update) -> bool:
    return update.effective_chat.id in CHAT_ID


async def image_func(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not is_allowed_chat(update):
        return
    #await update.message.reply_text("")

    try:

        with open("/backend/data_shared/requests/request.flag", 'w') as f:
            f.write("activar")
        
        time.sleep(5)
        #await asyncio.sleep(1)
            
        
        if os.path.exists("/backend/data_shared/requests/image.png"):

            with open("/backend/data_shared/requests/image.png", 'rb') as foto:
                await update.message.reply_photo(
                    photo=foto, 
                    caption="✅ Access Image"
                )
            
            os.remove("/backend/data_shared/requests/image.png")
            return
        

    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")

if __name__ == '__main__':
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    app.add_handler(CommandHandler("image", image_func))
    
    print("🤖 Bot")
    app.run_polling(drop_pending_updates=True)