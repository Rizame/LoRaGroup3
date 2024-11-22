import traceback
import paho.mqtt.client as mqtt
import base64
import json

MQTT_TOPIC = "v3/ibfkpj@ttn/devices/ibfklora/up"
MQTTSAX_TOPIC = "v3/project-software-engineering@ttn/devices/+/up"
MQTT_BROKER = "eu1.cloud.thethings.network"
MQTT_PORT = 1883

MQTT_USERNAME = "ibfkpj@ttn"
MQTTSAX_USERNAME = "project-software-engineering@ttn"

MQTT_PASSWORD = "NNSXS.LXYD6VAHPEP5VRUS7TIRO6K3OQA2KYEHS74CIFQ.ETFHW5J36LBAQ4ND6TYK4KV6MIEPGBT63M4VVEIN3M7M5US52NAQ" 
MQTTSAX_PASSWORD = "NNSXS.DTT4HTNBXEQDZ4QYU6SG73Q2OXCERCZ6574RVXI.CQE6IG6FYNJOO2MOFMXZVWZE4GXTCC2YXNQNFDLQL4APZMWU6ZGA"











def on_connectOWN(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to own device")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Connection failed with code {rc}")

def on_connectSAX(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to Saxion Devices")
        client.subscribe(MQTTSAX_TOPIC)
    else:
        print(f"Connection failed with code {rc}")        

def on_messageOWN(client, userdata, msg):
    try:
        #parsing
        payload = json.loads(msg.payload.decode('utf-8'))
        decoded_payload = base64.b64decode(payload["uplink_message"]["frm_payload"])
        print(f"Decoded payload: {decoded_payload}")
    
        if "uplink_message" in payload:
            b64_data = payload["uplink_message"]["frm_payload"]
            print(f"Base64 payload: {b64_data}")
            
            latitude = payload["uplink_message"]["rx_metadata"][0]["location"]["latitude"]
            longitude = payload["uplink_message"]["rx_metadata"][0]["location"]["longitude"]
            receivedAt = payload["received_at"]
            
            weatherDict = parseMKR(decoded_payload)
            pressure = weatherDict["pressure"]
            luminosity = weatherDict["luminosity"]
            temp = weatherDict["temperature"]
            humidity = weatherDict["humidity"]

            print("Temperature:", temp, "°C")
            print("Humidity:", humidity, "%")
            print("Luminosity:", luminosity, "Lux")
            print("Pressure:", pressure, "hPa")

            print(f"Latitude: {latitude}, Longitude: {longitude}")
            print(f"Latitude: {latitude}, Longitude: {longitude}")
            print(f"Received from: {payload["end_device_ids"]["device_id"]}")
            print(f"Received at: {receivedAt}")
            
                    
        else:
            print("uplink_message was not found in payload")

        

    except Exception as e:
        print(f"Error processing message: {e}")
        print(traceback.format_exc())


def parseMKR(decoded_payload):
    pressure = decoded_payload[0]/2+950
    luminosity = decoded_payload[1]
    temp = decoded_payload[2] + decoded_payload[3] / 10
    humidity = decoded_payload[4]
    return {
        'luminosity': luminosity,
        'temperature': temp,
        'humidity' : humidity,
        'pressure' : pressure
    }

def on_messageSAX(client, userdata, msg):
    try:
        #parsing
        payload = json.loads(msg.payload.decode('utf-8'))

    
        if "uplink_message" in payload:

            decoded_payload = base64.b64decode(payload["uplink_message"]["frm_payload"])
            print(f"Decoded payload: {decoded_payload}")

            if payload["uplink_message"]["version_ids"]["model_id"] =="lht65":
                latitude = payload["uplink_message"]["rx_metadata"][0]["location"]['latitude']
                longitude = payload["uplink_message"]["rx_metadata"][0]["location"]['longitude']
                receivedAt = payload["received_at"]
                
                temp = (decoded_payload[2] << 8 | decoded_payload[3]) / 100
                humidity = (decoded_payload[4] << 8 | decoded_payload[5]) / 10
                luminosity = 0
                externalTemp = 0
                if decoded_payload[6] == 1:
                    externalTemp = decoded_payload[7] << 8 | decoded_payload[8]
                    print(f"External temperature: {externalTemp}°C")
                elif decoded_payload[6] == 5:
                    luminosity = decoded_payload[7] << 8 | decoded_payload[8]
                    print(f"Luminosity: {luminosity}%")
                print("Temperature:", temp, "°C")
                print("Humidity:", humidity, "%")
                print("location:", latitude, "-", longitude)

                print(f"Received from: {payload["end_device_ids"]["device_id"]}")
                print(f"Received at: {receivedAt}")

            elif payload["uplink_message"]["version_ids"]["model_id"] =="mkr-wan-1310":
                latitude = payload["uplink_message"]["rx_metadata"][0]["location"]['latitude']
                longitude = payload["uplink_message"]["rx_metadata"][0]["location"]['longitude']
                receivedAt = payload["received_at"]
                
                weatherDict = parseMKR(decoded_payload)
                pressure = weatherDict["pressure"]
                luminosity = weatherDict["luminosity"]
                temp = weatherDict["temperature"]
                humidity = weatherDict["humidity"]

                print("Temperature:", temp, "°C")
                print("Humidity:", humidity, "%")
                print("Luminosity:", luminosity, "Lux")
                print("Pressure:", pressure, "hPa")

                print(f"Latitude: {latitude}, Longitude: {longitude}")
                print(f"Received from: {payload["end_device_ids"]["device_id"]}")
                print(f"Received at: {receivedAt}")
    

                    
        else:
            print("uplink_message was not found in payload")

        

    except Exception as e:
        print(f"Error processing message: {e}")
        print(traceback.format_exc())



clientOWN = mqtt.Client()
clientSAX = mqtt.Client()

clientOWN.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
clientSAX.username_pw_set(MQTTSAX_USERNAME, MQTTSAX_PASSWORD)

clientOWN.on_connect = on_connectOWN
clientOWN.on_message = on_messageOWN

clientSAX.on_connect = on_connectSAX
clientSAX.on_message = on_messageSAX

print("Connecting to TTN MQTT broker...")
clientOWN.connect(MQTT_BROKER)
clientSAX.connect(MQTT_BROKER)


#clientOWN.loop_start()
clientSAX.loop_start()



try:
    while True:
        pass
except KeyboardInterrupt:
    clientOWN.disconnect()
    clientSAX.disconnect()
    print("\nDisconnecting...")