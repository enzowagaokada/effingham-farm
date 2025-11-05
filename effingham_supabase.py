import json
import os
import paho.mqtt.client as mqtt
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
# --- Supabase Setup ---
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def on_connect(mqttc, obj, flags, rc):
    """Callback for when the client connects to the MQTT broker."""
    if rc == 0:
        print("Connected to MQTT Broker!")
        mqttc.subscribe("v3/+/devices/+/up", 0)
    else:
        print(f"Failed to connect to MQTT, return code {rc}")

def on_message(mqttc, obj, msg):
    """Callback for when a message is received from the broker."""
    try:
        payload = json.loads(msg.payload.decode('UTF-8'))
        
        device_eui = payload.get('end_device_ids', {}).get('device_id')
        uplink_message = payload.get('uplink_message', {})
        brand_name = uplink_message.get('version_ids', {}).get('brand_id')
        decoded_payload = uplink_message.get('decoded_payload')

        if not all([device_eui, brand_name, decoded_payload]):
            return # Skip if essential info is missing

        print(f"Processing message from Device: {device_eui}, Brand: {brand_name}")

        # --- Step 1: Ensure Brand and Device exist in DB ---
        supabase.table("Brands").upsert({"brand_name": brand_name}, on_conflict="brand_name").execute().data

        device_data = supabase.table("Devices").upsert({"device_eui": device_eui, "brand": brand_name}, on_conflict="device_eui").execute().data
        device_name = device_data[0]['sensor_name']

        # --- Step 2: Route and Insert sensor data based on brand ---

        # Handle Tektelic soil sensors
        if brand_name == "tektelic" and uplink_message.get('f_port') == 10:
            print("-> Tektelic sensor data found. Inserting into SoilSensorReadings.")
            data_to_insert = {
                "sensor_name": device_name,
                "ambient_temperature": decoded_payload.get('ambient_temperature'),
                "light_intensity": decoded_payload.get('light_intensity'),
                "relative_humidity": decoded_payload.get('relative_humidity'),
                "soil_temperature": decoded_payload.get('Input3_voltage_to_temp'),
                "soil_moisture": decoded_payload.get('watermark1_tension')
            }
            supabase.table("SoilSensorReadings").insert(data_to_insert).execute()
            print("-> Successfully inserted soil data.")

        # Handle Elsys climate sensors
        elif brand_name == "elsys":
            print("-> Elsys sensor data found. Inserting into ClimateReadings.")
            data_to_insert = {
                "sensor_name": device_name,
                "temperature": decoded_payload.get('temperature'),
                "humidity": decoded_payload.get('humidity'),
                "pressure": decoded_payload.get('pressure'),
                "co2": decoded_payload.get('co2')
            }
            supabase.table("ClimateReadings").insert(data_to_insert).execute()
            print("-> Successfully inserted climate data.")
        
        else:
            print(f"-> No specific handler for brand '{brand_name}'. Skipping data insert.")

    except Exception as e:
        print(f"An error occurred: {e}")

def on_subscribe(mqttc, obj, mid, granted_qos):
    """Callback for when the client successfully subscribes to a topic."""
    print(f"Subscribed: {mid} {granted_qos}")

def run_mqtt_listener():
    """Sets up and runs the MQTT client."""
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe
    mqttc.username_pw_set("gatech-effingham@ttn", "NNSXS.LGQMJICYDFLYT33BHKZSWH5NGFE4GJVNIW4GE3Y.LN45PC3GTPDMSBDBZLKHOEDSF7GLVYTJYIRBJ4JQVOXYFDVUQOUA")
    mqttc.connect("nam1.cloud.thethings.network", 1883, 60)

    try:
        print("Starting MQTT listener to ingest all data... Press Ctrl+C to stop.")
        mqttc.loop_forever()
    except KeyboardInterrupt:
        print("\nListener stopped by user.")
        mqttc.disconnect()

if __name__ == '__main__':
    run_mqtt_listener()