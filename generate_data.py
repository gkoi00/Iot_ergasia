import random
import datetime
import time
from kafka import KafkaProducer
import json
import redis

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

r = redis.Redis(host='localhost', port=6379, db = 0, decode_responses=True)
ts = r.ts()

class sensor:
    def __init__(self, name, interval, valueRangeStart, valueRangeEnd):
        self.name = name
        self.valueRangeStart = valueRangeStart
        self.valueRangeEnd = valueRangeEnd
        self.interval = interval

def generate_sensor_data(date, previousEtot, previousWtot):
    data = []

    for sensor in sensors:
        # Daily sensors skip
        if sensor.interval == 96 and date.time() != datetime.time(00,00):
            continue

        # Movement sensor skip
        if sensor.interval == 0:
            if random.random() < 0.95:
                continue

        value = round(random.uniform(sensor.valueRangeStart, sensor.valueRangeEnd),1)
        #value = 1
        if sensor.name == 'Etot':
            value = round(value + previousEtot, 1)
        if sensor.name == 'Wtot':
            value = round(value + previousWtot, 1)
        if sensor.name == 'Mov1':
            value = 1
        data.append((sensor.name, date.strftime('%Y-%m-%d %H:%M'), str(value)))

    return data

def generate_late_events(date, secCounter):
    late_data = []
    sensor = sensors[8]
    
    ### CHANGE TO 20 AND 120 SEC
    if secCounter % 20 == 0:
        lateDate = date - datetime.timedelta(days=2)
        if (lateDate < datetime.datetime(2020, 1, 1)):
            return late_data
        value = round(random.uniform(sensor.valueRangeStart, sensor.valueRangeEnd),1)
        #value = 1
        late_data.append((sensor.name, lateDate.strftime('%Y-%m-%d %H:%M'), str(value)))
    if secCounter % 120 == 0:
        lateDate = date - datetime.timedelta(days=10)
        if (lateDate < datetime.datetime(2020, 1, 1)):
            return late_data
        value = round(random.uniform(sensor.valueRangeStart, sensor.valueRangeEnd),1)
        #value = 1
        late_data.append((sensor.name, lateDate.strftime('%Y-%m-%d %H:%M'), str(value)))

    return late_data



sensors = [sensor('TH1', 1, 12, 35),
           sensor('TH2', 1, 12, 35),
           sensor('HVAC1', 1, 0, 100),
           sensor('HVAC2', 1, 0, 200),
           sensor('MiAC1', 1, 0, 150),
           sensor('MiAC2', 1, 0, 200),
           sensor('Etot', 96, 61400, 63400),
           sensor('Mov1', 0, 1, 1),
           sensor('W1', 1, 0, 1),
           sensor('Wtot', 96, 100, 120)]

for sensor in sensors:
    ts.create(sensor.name)
    if sensor.name == "Mov1":
        continue
    elif sensor.name == "Wtot" or sensor.name == "Etot":
        ts.create("AggDayDiff" + sensor.name)
    else:
        ts.create("AggDay" + sensor.name)
ts.create("LateW1")
ts.create("AggDayRestE")
ts.create("AggDayRestW")


date = datetime.datetime(2020, 1, 1)
previousEtot = 0
previousWtot = 0
secCounter = 1

while(True):
    date += datetime.timedelta(minutes=15)
    dataNormal = generate_sensor_data(date, previousEtot, previousWtot)
    dataLate = generate_late_events(date, secCounter)
    data = dataNormal + dataLate
    for entry in data:
        if entry[0] == 'Etot':
            prev = (entry[0], entry[1], str(previousEtot))
            timestamp = datetime.datetime.strptime(prev[1], '%Y-%m-%d %H:%M').timestamp()
            producer.send("iot_measurements", " ".join(prev), timestamp_ms=int(timestamp * 1000))
            previousEtot = float(entry[2])
        if entry[0] == 'Wtot':
            prev = (entry[0], entry[1], str(previousWtot))
            timestamp = datetime.datetime.strptime(prev[1], '%Y-%m-%d %H:%M').timestamp()
            producer.send("iot_measurements", " ".join(prev), timestamp_ms=int(timestamp * 1000))
            previousWtot = float(entry[2])
        print(" ".join(entry))
        timestamp = datetime.datetime.strptime(entry[1], '%Y-%m-%d %H:%M').timestamp()
        producer.send("iot_measurements", " ".join(entry), timestamp_ms=int(timestamp * 1000))
        ts.add(entry[0], int(timestamp)*1000, entry[2])
        #r.set(entry[0] + ": " + entry[1], entry[2])
    print("----------------------------------------------------------------------------------------")
    # Send to Kafka
    producer.flush()
    secCounter += 1
    time.sleep(0.08)
