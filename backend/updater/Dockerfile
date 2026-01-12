# backend/updater/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
#RUN apt-get update && apt-get install -y unzip


COPY . .


#CMD ["python", "main.py"]
CMD ["tail", "-f", "/dev/null"]