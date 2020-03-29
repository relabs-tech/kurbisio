#!/usr/bin/env python
import paho.mqtt.client as mqtt
import ssl
import requests
import os.path
import json
import time

# Use the executable filename as IMEI
imei = os.path.basename(__file__).split('.')[0]

# Authorize with backend.
print("authorize with backend: " + imei)
r = requests.get("http://localhost:3000/device-authorizations/"+imei, headers={"Kurbisio-Equipment-Key":"secret"}).json()
device_id = r["device_id"]
print("received kurbisio device_id: " + device_id)

# Store device_id and certificate
file = open(imei+".id","w") 
file.write(device_id)
file.close()
if r.has_key("cert"):
    print("got new cert and key")
    file = open(imei+".crt","w") 
    file.write(r["cert"])
    file.close()
    file = open(imei+".key","w") 
    file.write(r["key"])
    file.close()

# Read stored configuration
config = "basic config"
try:
    file = open(imei+".cfg","r")
    config = file.read()
except IOError:
    pass
finally:
    file.close()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("kurbisio/"+device_id+"/twin/requests/configuration")
    client.publish("kurbisio/"+device_id+"/twin/get", json.dumps(["configuration"]))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("received data on " + msg.topic + " (" + str(len(msg.payload)) + " bytes) " + str(msg.payload))
    if msg.topic == "kurbisio/"+device_id+"/twin/requests/configuration":
        global config
        if config != str(msg.payload):
            # here we should do something with the received configuration. But since this is a simple
            # example program, we simply store the configuration and wait 5 seconds
            print("received new configuration, updating...")
            config = str(msg.payload)
            file = open(imei+".cfg","w")
            file.write(config)
            file.close()
            time.sleep(5)
            print("...update done!")
        # confirm via report that we have received and handled the configuration
        client.publish("kurbisio/"+device_id+"/twin/reports/configuration", config, qos=0)

client = mqtt.Client(client_id=device_id, clean_session=True)
client.tls_set(ca_certs="../../ca.crt", tls_version=ssl.PROTOCOL_TLSv1_2, certfile=imei+".crt", keyfile=imei+".key")
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost",8883)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
