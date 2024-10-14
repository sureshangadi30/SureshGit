**Function to load MangoDB**
from pymongo import MongoClient
def load_data_to_mongodb(data, db_name='mydb', collection_name='employees'):
    client = MongoClient('mongodb://10.148.190.206:27017/')
    db = dev_greater_sydney_division_bronze
    collection = db[midt]
    collection.insert_many(data)

**Saving to JSON**
import json
def save_to_json(data, file_name='People.json'):
    with open(file_name, 'w') as f:
        json.dump(data, f, indent=4)

**Dockerize the Application**
FROM python:3.8-slim
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "etl.py"]
docker-compose.yml:
yaml
Copy code
version: '3'
services:
  etl-app:
    build: .
    depends_on:
      - mongodb
    volumes:
      - .:/app
    command: python etl.py
  mongodb:
    image: mongo:4.4.6
    ports:
      - 27017:27017
    volumes:
      - ./data/db:/data/db
      
