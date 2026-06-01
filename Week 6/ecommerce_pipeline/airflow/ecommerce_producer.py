from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['Shirt','Coat','Jeans','Skirt','Dress','Blouse','Socks','Scarf','Gloves','Hat']
regions = ['Europe', 'Asia', 'North America', 'South America', 'Africa', 'Oceania']

for i in range(10000):
    order = {
        "order_id": i + 1,
        "customer_name": fake.name(),
        "product": random.choice(products),
        "amount": round(random.uniform(10, 1000), 2), #price with 2 decimals
        "region": random.choice(regions),
        "timestamp": str(datetime.now())
    }
    producer.send("orders_ecommerce", value=order) #sends order to the topic "orders_ecommerce"
    time.sleep(0.01) #delay between orders  

print("10,000 order events sent")