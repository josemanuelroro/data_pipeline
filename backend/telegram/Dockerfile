FROM python:3.9-slim

COPY entrypoint.sh .
RUN chmod 777 entrypoint.sh
WORKDIR /app

COPY requirements.txt  .

RUN pip install --no-cache-dir -r requirements.txt


#COPY /scripts/watcher.py .

CMD ["../entrypoint.sh"]
#CMD ["python", "-u", "watcher.py"]