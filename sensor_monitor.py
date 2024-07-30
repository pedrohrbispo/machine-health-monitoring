import paho.mqtt.client as mqtt
import json
import time
import psutil
import sys
from datetime import datetime, timezone

BROKER_ADDRESS = "localhost"
MESSAGE_INTERVAL = 10

class SensorInfo:
    def __init__(self, sensor_id, data_type, data_interval):
        self.sensor_id = sensor_id
        self.data_type = data_type
        self.data_interval = data_interval

def get_cpu_usage():
    return psutil.cpu_percent(interval=1)

def get_memory_usage():
    mem = psutil.virtual_memory()
    return mem.percent

def send_initial_message(client, machine_id, sensors):
    initial_message = {
        "machine_id": machine_id,
        "sensors": []
    }

    for sensor in sensors:
        sensor_json = {
            "sensor_id": sensor.sensor_id,
            "data_type": sensor.data_type,
            "data_interval": sensor.data_interval
        }
        initial_message["sensors"].append(sensor_json)

    client.publish("/sensor_monitors", json.dumps(initial_message), qos=1)
    print("Initial message published:", json.dumps(initial_message))

def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("Connected to broker")
    else:
        print("Connection failed with code", rc)

def main(machine_id):
    client_id = f"sensor-monitor-{machine_id}"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)
    client.on_connect = on_connect
    client.connect(BROKER_ADDRESS)

    client.loop_start()

    sensors = [
        SensorInfo("cpu_usage", "float", 5000),  # CPU usage sensor
        SensorInfo("memory_usage", "float", 5000)  # Memory usage sensor
    ]

    send_initial_message(client, machine_id, sensors)

    while True:
        timestamp = datetime.now(timezone.utc).isoformat()

        # Get sensor readings
        cpu_usage = get_cpu_usage()
        memory_usage = get_memory_usage()

        # Publish CPU usage to MQTT
        cpu_json = {
            "timestamp": timestamp,
            "value": cpu_usage
        }
        cpu_topic = f"/sensors/{machine_id}/cpu_usage"
        client.publish(cpu_topic, json.dumps(cpu_json), qos=1)
        print(f"CPU message published - topic: {cpu_topic} - message: {json.dumps(cpu_json)}")

        # Publish memory usage to MQTT
        memory_json = {
            "timestamp": timestamp,
            "value": memory_usage
        }
        memory_topic = f"/sensors/{machine_id}/memory_usage"
        client.publish(memory_topic, json.dumps(memory_json), qos=1)
        print(f"Memory message published - topic: {memory_topic} - message: {json.dumps(memory_json)}")

        time.sleep(MESSAGE_INTERVAL)

if __name__ == "__main__":
    machine_id = "cpu-sensors"
    main(machine_id)
