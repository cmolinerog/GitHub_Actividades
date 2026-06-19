import json
import time
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# 1. Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'iot_data'
sensors = ['SENSOR_01', 'SENSOR_02', 'SENSOR_03']

print("🚀 Producer started. Sending data (including deliberate late events)...")

try:
    while True:
        sensor_id = random.choice(sensors)
        temperature = round(random.uniform(20.0, 30.0), 2)
        
        # --- LATE DATA SIMULATION ---
        is_late_data = random.random() < 0.15
        
        if is_late_data:
            minutes_back = random.randint(12, 15)
            event_timestamp = datetime.now(timezone.utc) - timedelta(minutes=minutes_back)
            print(f"⚠️ [LATE DATA] {sensor_id} sent with a {minutes_back} min delay.")
        else:
            event_timestamp = datetime.now(timezone.utc)
            print(f"✅ [REAL TIME] {sensor_id}: {temperature}°C")
            
        # Format the timestamp directly to ISO-8601 format
        formatted_timestamp = event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # 2. Create JSON Payload (Changed key from ts to timestamp)
        payload = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "timestamp": formatted_timestamp  # <-- Renamed to timestamp
        }
        
        # 3. Send to Kafka
        producer.send(topic_name, value=payload)
        
        # Wait 2 seconds before sending the next event
        time.sleep(2)

except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")
finally:
    producer.close()