import os
import json
import paho.mqtt.client as mqtt
from pathlib import Path
import yaml

# Load config
cfg_path = os.path.join( "config_m3.yaml")
with open(cfg_path) as f:
    cfg = yaml.safe_load(f)

MQTT_HOST = cfg['mqtt']['host']
MQTT_PORT = cfg['mqtt']['port']
LIGHT_TOPIC_PREFIX = cfg['lighting_control']['lighting_topic_prefix']

STATE_FILE = Path("./lights_state.json")

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

def on_connect(client, userdata, flags, rc):
    print(f"Actuator connected, subscribing to {LIGHT_TOPIC_PREFIX}/+")
    client.subscribe(f"{LIGHT_TOPIC_PREFIX}/+")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    try:
        data = json.loads(payload)
    except:
        data = {"raw": payload}
    print("Actuator received:", data)

    # update state
    current = {}
    if STATE_FILE.exists():
        current = json.loads(STATE_FILE.read_text())

    street = data.get("street", "unknown")
    level = data.get("level", "LOW")
    
    current[street] = level
    save_state(current)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_forever()