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

# Stream each row to Kafka
for _, row in df.iterrows():
    event = row.to_dict()
    producer.send("logistics-events", event)
    print("Sent:", event)
    time.sleep(180)  # simulate 1-second delay between messages

producer.close()
