from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load your dataset
df = pd.read_csv("../data/logistics_data.csv")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:19092','localhost:29092','localhost:39092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for counter, (_, row) in enumerate(df.iterrows(), start=1):
    event = row.to_dict()
    producer.send("logistics-events", event)
    print(f"{counter}")
    time.sleep(2)

producer.close()
