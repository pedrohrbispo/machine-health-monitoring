import json
import time
import threading
from datetime import datetime, timezone
import numpy as np
import paho.mqtt.client as mqtt
from collections import deque
import psycopg2
from dateutil import parser
from typing import Deque

QOS = 1
BROKER_ADDRESS = "localhost"
MOVING_AVERAGE_WINDOW = 5

# Definição de cores ANSI para o console
RESET = "\033[0m"
RED = "\033[31m"

# Configuração do PostgreSQL
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "1234"

def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_table_if_not_exists():
    conn = connect_to_db()
    cursor = conn.cursor()
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS metrics (
        id SERIAL PRIMARY KEY,
        machine_id VARCHAR(255) NOT NULL,
        sensor_id VARCHAR(255) NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION NOT NULL
    )
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def post_metric(machine_id, sensor_id, timestamp_str, value):
    conn = connect_to_db()
    cursor = conn.cursor()
    
    insert_query = '''
    INSERT INTO metrics (machine_id, sensor_id, timestamp, value)
    VALUES (%s, %s, %s, %s)
    '''
    timestamp = parser.parse(timestamp_str).replace(tzinfo=timezone.utc)  # Garantir que o datetime seja aware
    cursor.execute(insert_query, (machine_id, sensor_id, timestamp, value))
    
    conn.commit()
    cursor.close()
    conn.close()
    return 0

def calculateMovingAverage(values: Deque[float]) -> float:
    if not values:
        return 0.0  # Se a fila estiver vazia, retorna 0 como média móvel

    total = sum(values)
    return total / len(values)

def calculateZScore(value: float, values: Deque[float]) -> float:
    if not values:
        return 0.0  # Se a fila estiver vazia, retorna 0 como Z-score

    mean = calculateMovingAverage(values)
    variance = sum((val - mean) ** 2 for val in values) / len(values)
    std_deviation = np.sqrt(variance)

    if std_deviation == 0.0:
        return 0.0  # Retorna 0 se o desvio padrão for zero para evitar divisão por zero

    return (value - mean) / std_deviation

def calculateTrend(values: Deque[float]) -> float:
    n = len(values)
    sum_x = 0.0
    sum_y = 0.0
    sum_xy = 0.0
    sum_xx = 0.0

    if n < 2:
        return 0.0

    for i in range(n):
        x = i + 1
        y = values[i]
        sum_x += x
        sum_y += y
        sum_xy += x * y
        sum_xx += x * x

    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)

    return slope

def process_sensor_data(machine_id, sensor_id, timestamp, value, sensorData, sensorTimestamp):
    print(f"\n\n--------------------->    Análise de dados para o sensor {sensor_id}   <---------------------")
    time = parser.parse(timestamp).replace(tzinfo=timezone.utc)  # Garantir que o datetime seja aware
    print(f"Data: {time.strftime('%d/%m/%Y, Hora: %H:%M:%S')}")
    print(f"ID da máquina: {machine_id}")

    sensorData.append(value)
    if len(sensorData) > MOVING_AVERAGE_WINDOW:
        sensorData.popleft()  # Usar popleft() para deque

    if sensorData:
        movingAverage = calculateMovingAverage(sensorData)
        print(f"Média móvel do uso de {sensor_id}: {movingAverage}")
        post_metric(machine_id, f"{sensor_id}.{sensor_id}_moving_average", timestamp, movingAverage)

        zScore = calculateZScore(value, sensorData)
        zScoreThreshold = 1.0

        if abs(zScore) > zScoreThreshold:
            print(f"{RED}[ALARME]{RESET} Outlier detectado: {value}")
            post_metric(machine_id, f"alarms.{sensor_id}_outlier", timestamp, 1)
        else:
            print(f"Uso normal de {sensor_id}: {value}")

        trend = calculateTrend(sensorData)
        print(f"Tendência do uso de {sensor_id}: {trend}")
        post_metric(machine_id, f"{sensor_id}.{sensor_id}_trend", timestamp, trend)
        print("----------------------------------------------------------------------------------------------")

cpuUsageData = deque()
def process_sensor_data_cpu(machine_id, sensor_id, timestamp, value):
    global cpuUsageData
    process_sensor_data(machine_id, sensor_id, timestamp, value, cpuUsageData, None)

memUsageData = deque()
def process_sensor_data_mem(machine_id, sensor_id, timestamp, value):
    global memUsageData
    process_sensor_data(machine_id, sensor_id, timestamp, value, memUsageData, None)

def check_sensor_inactivity(machine_id, sensor_id, sensor_name):
    expected_interval_seconds = 30
    max_expected_delay = expected_interval_seconds

    while True:
        time.sleep(20)
        current_time = datetime.utcnow().replace(tzinfo=timezone.utc)  # Garantir que o datetime seja aware
        with threading.Lock():
            last_timestamp = next((sensor['last_timestamp'] for sensor in monitored_sensors if sensor['sensor_id'] == sensor_id), None)
            if last_timestamp:
                last_time = parser.parse(last_timestamp).replace(tzinfo=timezone.utc)  # Garantir que o datetime seja aware
                seconds_since_last_timestamp = (current_time - last_time).total_seconds()

                if seconds_since_last_timestamp > max_expected_delay:
                    print(f"{RED}\n⚠️ - [ALARME] {RESET}Dados do sensor {sensor_name} da máquina {machine_id} não foram recebidos por mais de 10 períodos de tempo previstos.")
                    post_metric(machine_id, f"alarms.inactive_{sensor_name}", current_time.strftime("%Y-%m-%dT%H:%M:%S"), 1)

monitored_sensors = []

def add_monitored_sensor(sensor_id, timestamp):
    with threading.Lock():
        monitored_sensors.append({'sensor_id': sensor_id, 'last_timestamp': timestamp})

def is_sensor_monitored(sensor_id):
    with threading.Lock():
        return any(sensor['sensor_id'] == sensor_id for sensor in monitored_sensors)

def process_message(machine_id, sensor_id, timestamp, sensor_name):
    if not is_sensor_monitored(sensor_id):
        threading.Thread(target=check_sensor_inactivity, args=(machine_id, sensor_id, sensor_name)).start()
        add_monitored_sensor(sensor_id, timestamp)

class MqttCallback:
    def __init__(self, client):
        self.client = client

    def on_message(self, client, userdata, msg):
        j = json.loads(msg.payload.decode())
        topic = msg.topic.split('/')
        machine_id = topic[2]
        sensor_id = topic[3]
        timestamp = j["timestamp"]
        value = j["value"]

        post_metric(machine_id, f"{sensor_id}.{sensor_id}", timestamp, value)
        if sensor_id == "cpu_usage":
            process_sensor_data_cpu(machine_id, sensor_id, timestamp, value)
            process_message(machine_id, f"{sensor_id}{machine_id}", timestamp, sensor_id)
        else:
            process_sensor_data_mem(machine_id, sensor_id, timestamp, value)
            process_message(machine_id, f"{sensor_id}{machine_id}", timestamp, sensor_id)

def main():
    create_table_if_not_exists()

    client_id = "clientId"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)

    callback = MqttCallback(client)
    client.on_message = callback.on_message

    client.connect(BROKER_ADDRESS)
    client.subscribe("/sensors/#", QOS)

    print("Subscribed")

    client.loop_start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
