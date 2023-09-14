import redis
from kafka import KafkaConsumer
import json
import datetime

r = redis.Redis(host='localhost', port=6379, db = 0, decode_responses=True)
ts = r.ts()

consumer = KafkaConsumer(
    'Aggregations',
     bootstrap_servers='localhost:9092',
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: x.decode('utf-8'))

for message in consumer:
    entry = message.value.split()
    if entry[0] == "Other":
        continue
    timestamp = datetime.datetime.strptime(entry[1] + " " + entry[2], '%Y-%m-%d %H:%M').timestamp()
    ts.add(entry[0], int(timestamp)*1000, float(entry[3]))
