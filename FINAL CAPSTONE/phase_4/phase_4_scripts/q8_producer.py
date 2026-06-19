import csv
import json
from kafka import KafkaProducer

# 1. Configuración básica del productor
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'  # <- Esto cumple con el requisito de "at-least-once"
)

topic_name = 'phase4_q8'
csv_file_path = 'sample_data_sensor.csv'

print("Iniciando el envío de datos...")

try:
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for fila in reader:
            # Al usar 'value_serializer', ya no necesitas hacer json.dumps aquí.
            # Le pasas el diccionario directamente y la librería lo convierte solo.
            producer.send(topic=topic_name, value=fila)
            
except FileNotFoundError:
    print(f"Error: El archivo {csv_file_path} no existe.")
except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    # Asegura que todos los mensajes en memoria se envíen antes de cerrar
    print("Vaciando el búfer de mensajes (flush)...")
    producer.flush()
    producer.close()
    print("¡Proceso terminado!")