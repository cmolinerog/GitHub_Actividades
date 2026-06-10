#!/usr/bin/env python3
"""
Sensor Data Generator for IoT Monitoring Pipeline
Generates realistic sensor readings and publishes to Kafka
"""

import json
import random
import time
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Try to import kafka, with helpful error message
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python not installed. Run: pip install kafka-python")
    print("    Falling back to file output mode")


class SensorDataGenerator:
    def __init__(self, use_kafka=True, kafka_broker="localhost:9092"):
        self.use_kafka = use_kafka and KAFKA_AVAILABLE
        self.kafka_broker = kafka_broker
        self.producer = None
        
        if self.use_kafka:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[kafka_broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=10000
                )
                print(f"✓ Connected to Kafka: {kafka_broker}")
            except Exception as e:
                print(f"✗ Kafka connection failed: {e}")
                print("  Falling back to file output")
                self.use_kafka = False
        
        # Sensor configurations
        self.sensors = {
            "S001": {"location": "Downtown", "temp_range": (25, 35), "humidity_range": (55, 75)},
            "S002": {"location": "Airport", "temp_range": (18, 28), "humidity_range": (40, 65)},
            "S003": {"location": "Suburb", "temp_range": (20, 32), "humidity_range": (50, 80)},
            "S004": {"location": "Hospital", "temp_range": (22, 26), "humidity_range": (45, 70)},
            "S005": {"location": "Mall", "temp_range": (18, 24), "humidity_range": (50, 70)}
        }
        
        self.anomaly_sensors = ["S004"]  # Hospital sensor occasionally has high temps (anomaly)
    
    def generate_reading(self, sensor_id, base_time):
        """Generate a single sensor reading"""
        config = self.sensors[sensor_id]
        
        # Generate anomaly occasionally
        if sensor_id in self.anomaly_sensors and random.random() < 0.15:
            temperature = random.uniform(35, 42)  # Anomaly temperature
        else:
            temp_min, temp_max = config["temp_range"]
            temperature = random.uniform(temp_min, temp_max)
        
        humidity_min, humidity_max = config["humidity_range"]
        humidity = random.uniform(humidity_min, humidity_max)
        
        # Air quality (ppm)
        air_quality = random.randint(20, 100)
        if sensor_id in self.anomaly_sensors and random.random() < 0.1:
            air_quality = random.randint(150, 200)  # Poor quality
        
        reading = {
            "sensor_id": sensor_id,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "air_quality": air_quality,
            "location": config["location"],
            "timestamp": base_time.isoformat() + "Z"
        }
        
        return reading
    
    def generate_batch(self, count=10, output_file=None):
        """Generate a batch of readings"""
        readings = []
        base_time = datetime.utcnow()
        
        for i in range(count):
            sensor_id = random.choice(list(self.sensors.keys()))
            current_time = base_time + timedelta(seconds=i * 5)
            reading = self.generate_reading(sensor_id, current_time)
            readings.append(reading)
            
            # Send to Kafka or file
            if self.use_kafka and self.producer:
                try:
                    self.producer.send("sensors_raw", value=reading)
                except Exception as e:
                    print(f"✗ Error sending to Kafka: {e}")
            elif output_file:
                readings.append(reading)
        
        if self.use_kafka and self.producer:
            self.producer.flush()
        
        return readings
    
    def continuous_stream(self, interval=5, count=None):
        """Generate continuous stream of readings"""
        generated = 0
        
        print(f"Starting continuous sensor stream (interval={interval}s)")
        print("Press Ctrl+C to stop")
        
        try:
            while count is None or generated < count:
                sensor_id = random.choice(list(self.sensors.keys()))
                reading = self.generate_reading(sensor_id, datetime.utcnow())
                
                if self.use_kafka and self.producer:
                    self.producer.send("sensors_raw", value=reading)
                    print(f"[{generated}] Sent: {sensor_id} @ {reading['location']}: "
                          f"{reading['temperature']}°C, {reading['humidity']}%")
                else:
                    print(f"[{generated}] Generated: {json.dumps(reading)}")
                
                generated += 1
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print(f"\n✓ Generated {generated} readings")
            if self.use_kafka and self.producer:
                self.producer.flush()
                self.producer.close()
    
    def export_to_csv(self, filename, count=100):
        """Export readings to CSV file"""
        import csv
        
        readings = self.generate_batch(count)
        
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=readings[0].keys())
            writer.writeheader()
            writer.writerows(readings)
        
        print(f"✓ Exported {count} readings to {filename}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Sensor Data Generator")
    parser.add_argument("--kafka-broker", default="localhost:9092", 
                       help="Kafka broker address")
    parser.add_argument("--no-kafka", action="store_true",
                       help="Skip Kafka and use file output only")
    parser.add_argument("--batch", type=int, default=10,
                       help="Generate batch of N readings")
    parser.add_argument("--stream", action="store_true",
                       help="Generate continuous stream")
    parser.add_argument("--interval", type=int, default=5,
                       help="Interval between readings (seconds)")
    parser.add_argument("--count", type=int, default=None,
                       help="Total readings to generate (default: infinite)")
    parser.add_argument("--csv", type=str,
                       help="Export to CSV file")
    
    args = parser.parse_args()
    
    generator = SensorDataGenerator(
        use_kafka=not args.no_kafka,
        kafka_broker=args.kafka_broker
    )
    
    if args.csv:
        generator.export_to_csv(args.csv, count=args.batch)
    elif args.stream:
        generator.continuous_stream(interval=args.interval, count=args.count)
    else:
        readings = generator.generate_batch(count=args.batch)
        print(f"\n✓ Generated {len(readings)} readings:\n")
        for reading in readings[:5]:  # Show first 5
            print(json.dumps(reading, indent=2))
        if len(readings) > 5:
            print(f"... and {len(readings) - 5} more readings")


if __name__ == "__main__":
    main()
