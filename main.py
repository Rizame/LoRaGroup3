import pyodbc
import traceback
import paho.mqtt.client as mqtt
import base64
import json

# Connection details
server = 'group13.database.windows.net,1433'
database = 'weather_state'
username = 'cloudadmin'
password = 'Group13pass'

# Connection string using ODBC Driver 18
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

# Connect to the database
conn = pyodbc.connect(connection_string)



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
        battery_voltage = None
        if "uplink_message" in payload:
            b64_data = payload["uplink_message"]["frm_payload"]
            deviceID = payload["end_device_ids"]["device_id"]
            modelID = payload["uplink_message"]["version_ids"]["model_id"]
            rssi = payload["uplink_message"]["rx_metadata"][0]["rssi"]
            print(f"Base64 payload: {b64_data}")
            
            latitude = payload["uplink_message"]["rx_metadata"][0]["location"]["latitude"]
            longitude = payload["uplink_message"]["rx_metadata"][0]["location"]["longitude"]
            altitude = payload["uplink_message"]["rx_metadata"][0]["location"]["altitude"]
            gateway = payload["uplink_message"]["rx_metadata"][0]["gateway_ids"]["gateway_id"]
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
            print(f"Received from: {payload['end_device_ids']['device_id']}")

            print(f"Received at: {receivedAt}")
            deviceID = payload["end_device_ids"]["device_id"]
            modelID = payload["uplink_message"]["version_ids"]["model_id"]
            snr = payload["uplink_message"]["rx_metadata"][0]["snr"]
            #Create a cursor
            cursor = conn.cursor()
            #Check the existence of device
            deviceID = "own"
            query = "SELECT 1 FROM device WHERE deviceID = ?"
            cursor.execute(query, (deviceID,))
            device_exists = cursor.fetchone()
            #insert device if not existing
            if not device_exists:
                cursor.execute("""INSERT INTO device(deviceID, longitude, latitude, altitude, gateway, modelID, battery_voltage)  VALUES (?, ?, ?, ?, ?, ?, ?) """, 
                (deviceID, longitude, latitude, altitude, gateway, modelID, battery_voltage))
            else:
                cursor.execute("""
                UPDATE device
                SET 
                    modelID = ?,
                    longitude = ?,
                    latitude = ?,
                    altitude = ?,
                    gateway = ?,
                    battery_voltage = ?
                WHERE deviceID = ?;
                """, (modelID, longitude, latitude, altitude, gateway, battery_voltage, deviceID))

            query = "SELECT 1 FROM gateway WHERE gatewayID = ?"
            cursor.execute(query, (gateway,))
            gateweay_exists = cursor.fetchone()
            if not gateweay_exists:
                cursor.execute(""" INSERT INTO gateway(gatewayID, latitude, longitude, altitude, avg_rssi, snr, max_rssi, min_rssi) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""", (gateway, latitude, longitude, altitude, rssi, snr, rssi, rssi))
            else:
                cursor.execute("SELECT MAX(max_rssi), MIN(min_rssi), rssi FROM your_table")
                max_rssi, min_rssi, average_rssi = cursor.fetchone()
                if(rssi > max_rssi):
                    cursor.execute("UPDATE gateway SET maxRssi = ?", (rssi))
                elif(rssi < min_rssi):
                    cursor.execute("UPDATE gateway SET minRssi = ?", (rssi))
                average_rssi = (rssi * 10 + average_rssi)/11
                cursor.execute("UPDATE gateway SET avg_rssi = ?", (average_rssi))

            cursor.execute("""INSERT INTO weather(humidity, luminosity, pressure, temperature, date, deviceID) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (humidity, luminosity, pressure, temp, receivedAt, deviceID))
            #insert weather data
            
            cursor.commit()
            cursor.close()
            print("Inserted succesfuly")
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
        pressure = None
        battery_voltage = None
        battery_percentage = None
        if "uplink_message" in payload:

            decoded_payload = base64.b64decode(payload["uplink_message"]["frm_payload"])
            print(f"Decoded payload: {decoded_payload}")

            if payload["uplink_message"]["version_ids"]["model_id"] =="lht65":
                latitude = payload["uplink_message"]["rx_metadata"][0]["location"]['latitude']
                longitude = payload["uplink_message"]["rx_metadata"][0]["location"]['longitude']
                altitude = payload["uplink_message"]["rx_metadata"][0]["location"]['altitude']
                battery_voltage = payload["uplink_message"]["decoded_payload"]["BatV"]
                receivedAt = payload["received_at"]
                
                battery_percentage = (battery_voltage-2.0) / 1.6 * 100
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

                print(f"Received from: {payload['end_device_ids']['device_id']}")
                print(f"Received at: {receivedAt}")

            elif payload["uplink_message"]["version_ids"]["model_id"] =="mkr-wan-1310":
                latitude = payload["uplink_message"]["rx_metadata"][0]["location"]['latitude']
                longitude = payload["uplink_message"]["rx_metadata"][0]["location"]['longitude']
                altitude = payload["uplink_message"]["rx_metadata"][0]["location"]['altitude']
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
                print(f"Received from: {payload['end_device_ids']['device_id']}")
                print(f"Received at: {receivedAt}")
            rssi = payload["uplink_message"]["rx_metadata"][0]["rssi"]
            snr = payload["uplink_message"]["rx_metadata"][0]["snr"]
            gateway = payload["uplink_message"]["rx_metadata"][0]["gateway_ids"]["gateway_id"]
            deviceID = payload["end_device_ids"]["device_id"]
            modelID = payload["uplink_message"]["version_ids"]["model_id"]
           
            cursor = conn.cursor()
            #check the existance of device (referenced from gpt)
            query = "SELECT 1 FROM device WHERE deviceID = ?"
            cursor.execute(query, (deviceID,))
            device_exists = cursor.fetchone()
 
            #insert/update device
            if not device_exists:
                cursor.execute(""" INSERT INTO device(deviceID, modelID, battery_voltage, battery_percentage) 
                VALUES (?, ?, ?, ?)""", (deviceID, modelID, battery_voltage, battery_percentage))
                print("Insert data to device tabl successfully")
            else:
                cursor.execute("""
                UPDATE device
                SET 
                    modelID = ?,
                    battery_voltage = ?,
                    battery_percentage = ?
                WHERE deviceID = ?;
                """, (modelID, battery_voltage, battery_percentage, deviceID))
                print("update device data successfully")
                
            #insert/update data
            query = "SELECT 1 FROM gateway WHERE gatewayID = ? and deviceID = ?"
            cursor.execute(query, (gateway,deviceID))
            gateweay_exists = cursor.fetchone()
            if not gateweay_exists:
                cursor.execute(""" INSERT INTO 
                gateway(gatewayID,deviceID, latitude, longitude, altitude, avg_rssi, avg_snr, max_rssi, min_rssi) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (gateway, deviceID, latitude, longitude, altitude, rssi, snr, rssi, rssi))
                print("insert to gateway successfully")
            else:
                cursor.execute("SELECT max_rssi, min_rssi, avg_rssi, avg_snr FROM gateway WHERE gatewayID = ? AND deviceID = ? ", (gateway, deviceID))
                max_rssi, min_rssi, average_rssi, average_snr = cursor.fetchone()
                if(rssi > max_rssi):
                    max_rssi = rssi
                elif(rssi < min_rssi):
                    min_rssi = rssi
                average_rssi = (rssi * 10 + average_rssi)/11
                average_snr = (snr*10 + average_snr)/11
                cursor.execute("UPDATE gateway SET avg_rssi = ?", (average_rssi))
                cursor.execute("""UPDATE gateway
                SET avg_rssi = ?, avg_snr = ?, max_rssi = ?, min_rssi = ?
                WHERE gatewayID = ? AND deviceID = ?
                """, (average_rssi, average_snr, max_rssi, min_rssi, gateway, deviceID))
                print("Gateway data updated successfully.")

            #insert weather
            cursor.execute("""INSERT INTO weather(humidity, luminosity, pressure, temperature, date, deviceID) 
            VALUES (?, ?, ?, ?, ?, ?)""", (humidity, luminosity, pressure, temp, receivedAt, deviceID))
            print("insert to weather successfully")
            cursor.commit()
            cursor.close()
            print("Inserted succesfuly")
            

          
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


clientOWN.loop_start()
clientSAX.loop_start()
print("")


try:
    while True:
        pass
except KeyboardInterrupt:
    clientOWN.disconnect()
    clientSAX.disconnect()
    print("\nDisconnecting...")