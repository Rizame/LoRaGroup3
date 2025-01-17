import pyodbc
import traceback
import paho.mqtt.client as mqtt
import base64
import json
import math

# Connection details
server = 'group13.database.windows.net,1433'
database = 'weather_state'
username = 'cloudadmin'
password = 'Group13pass'

# Connection string using ODBC Driver 18
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

# Connect to the database
conn = pyodbc.connect(connection_string)



MQTT_TOPIC = "v3/ibfkpj@ttn/devices/ibfkloranew/up"
MQTTSAX_TOPIC = "v3/project-software-engineering@ttn/devices/+/up"
MQTT_BROKER = "eu1.cloud.thethings.network"
MQTT_PORT = 1883

MQTT_USERNAME = "ibfkpj@ttn"
MQTTSAX_USERNAME = "project-software-engineering@ttn"

MQTT_PASSWORD = "NNSXS.LWGI7G24XBYHC6LLPMNRW6CISCJ7WYMG7X3NIJY.74GPZWD52EEVEOYRA5LZUUXYNSCUNO6K6FOSNCFL4C3PIEB67BEA" 
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
        externalTemp = None
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
            rssi = payload["uplink_message"]["rx_metadata"][0]["rssi"]
            snr = payload["uplink_message"]["rx_metadata"][0]["snr"]
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
            luminosity_percentage = 0
            if(luminosity!= 0):
                luminosity_percentage = math.log(luminosity)/math.log(660)*100
            #Create a cursor
            cursor = conn.cursor()
            #Check the existence of device
            query = "SELECT 1 FROM device WHERE deviceID = ?"
            cursor.execute(query, (deviceID,))
            device_exists = cursor.fetchone()
 
            #insert/update device
            if not device_exists:
                cursor.execute(""" INSERT INTO device(deviceID, modelID, battery_voltage, battery_percentage) 
                VALUES (?, ?, ?, ?)""", (deviceID, modelID, None, None))
                print("Insert data to device tabl successfully")
        

            #insert/update gateway
            query = "SELECT 1 FROM gateway WHERE gatewayID = ? and deviceID = ?"
            cursor.execute(query, (gateway,deviceID))
            gateweay_exists = cursor.fetchone()
            if not gateweay_exists:
                cursor.execute(""" INSERT INTO 
                gateway(gatewayID,deviceID, latitude, longitude, altitude, avg_rssi, avg_snr, max_rssi, min_rssi, max_snr, min_snr) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (gateway, deviceID, latitude, longitude, altitude, rssi, snr, rssi, rssi, snr, snr))
                print("insert to gateway successfully")
            else:
                cursor.execute("SELECT max_rssi, min_rssi, max_snr, min_snr, avg_rssi, avg_snr FROM gateway WHERE gatewayID = ? AND deviceID = ? ", (gateway, deviceID))
                max_rssi, min_rssi, max_snr, min_snr, average_rssi, average_snr = cursor.fetchone()
                if(rssi > max_rssi):
                    max_rssi = rssi
                elif(rssi < min_rssi):
                    min_rssi = rssi
                if(snr > max_snr):
                    max_snr = snr
                elif(snr < min_snr):
                    min_snr = snr
                average_rssi = (rssi * 10 + average_rssi)/11
                average_snr = (snr*10 + average_snr)/11
                cursor.execute("""UPDATE gateway
                SET avg_rssi = ?, avg_snr = ?, max_rssi = ?, min_rssi = ?, max_snr = ?, min_snr = ?
                WHERE gatewayID = ? AND deviceID = ?
                """, (average_rssi, average_snr, max_rssi, min_rssi, max_snr, min_snr, gateway, deviceID))
            #insert weather date
            cursor.execute("""INSERT INTO weather(humidity, luminosity, pressure, inside_temperature, external_temperature, date, deviceID) 
            VALUES (?, ?, ?, ?, ?, ?, ?)""", (humidity, luminosity_percentage, pressure, temp, externalTemp, receivedAt, deviceID))
            print("insert to weather successfully")
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
        externalTemp = None
        luminosity_percentage = 0
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
                luminosity = None

                if decoded_payload[6] == 1:
                    externalTemp = (decoded_payload[7] << 8 | decoded_payload[8]) / 100

                    print(f"External temperature: {externalTemp}°C")
                elif decoded_payload[6] == 5:
                    luminosity = decoded_payload[7] << 8 | decoded_payload[8]
                    print(f"math(log): {luminosity}%")
                    if(luminosity!= 0):
                        luminosity_percentage = math.log(luminosity)/math.log(65535)*100

                    print(f"Luminosity: {luminosity}%")
                    externalTemp = temp
                    temp = None
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
                if(temp < 12.0):
                    externalTemp = temp
                    temp = None

                humidity = weatherDict["humidity"]
                print(f"math(log): {luminosity}%")
                if(luminosity!= 0):
                    luminosity_percentage = math.log(luminosity)/math.log(255) * 100
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
            #insert/update device
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
                    battery_voltage = ?,
                    battery_percentage = ?
                WHERE deviceID = ?;
                """, (battery_voltage, battery_percentage, deviceID))
                print("update device data successfully")
                
            #insert/update gateway
            query = "SELECT 1 FROM gateway WHERE gatewayID = ? and deviceID = ?"
            cursor.execute(query, (gateway,deviceID))
            gateweay_exists = cursor.fetchone()
            if not gateweay_exists:
                cursor.execute(""" INSERT INTO 
                gateway(gatewayID,deviceID, latitude, longitude, altitude, avg_rssi, avg_snr, max_rssi, min_rssi) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (gateway, deviceID, latitude, longitude, altitude, rssi, snr, rssi, rssi))
                print("insert to gateway successfully")
            else:
                cursor.execute("SELECT max_rssi, min_rssi, max_snr, min_snr, avg_rssi, avg_snr FROM gateway WHERE gatewayID = ? AND deviceID = ? ", (gateway, deviceID))
                max_rssi, min_rssi, max_snr, min_snr, average_rssi, average_snr = cursor.fetchone()
                if(rssi > max_rssi):
                    max_rssi = rssi
                elif(rssi < min_rssi):
                    min_rssi = rssi
                average_rssi = (rssi * 10 + average_rssi)/11
                average_snr = (snr*10 + average_snr)/11
                if(snr > max_snr):
                    max_snr = snr
                elif(snr < min_snr):
                    min_snr = snr
                
                cursor.execute("""UPDATE gateway
                SET avg_rssi = ?, avg_snr = ?, max_rssi = ?, min_rssi = ?, max_snr = ?, min_snr = ?
                WHERE gatewayID = ? AND deviceID = ?
                """, (average_rssi, average_snr, max_rssi, min_rssi, max_snr, min_snr, gateway, deviceID))
                print("Gateway data updated successfully.")

            #insert weather
            cursor.execute("""INSERT INTO weather(humidity, luminosity, pressure, inside_temperature, external_temperature, date, deviceID) 
            VALUES (?, ?, ?, ?, ?, ?, ?)""", (humidity, luminosity_percentage, pressure, temp,externalTemp,receivedAt, deviceID))
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