#!/usr/bin/env python
import paho.mqtt.client as mqtt
import ssl
import requests
import os.path
import json
import time

# Use the executable filename as thing identifier
thing = os.path.basename(__file__).split('.')[0]

device_id = ""

print("check device credentials from backend")
r = requests.get("http://localhost:3000/credentials", 
    headers={
        "Kurbisio-Thing-Key":"secret",
        "Kurbisio-Thing-Identifier": thing
    })
if r.status_code == 200: # OK
    file = open(thing+".cred","w") 
    file.write(r.text)
    file.close()
    credentials = r.json()
    print("received new credentials")
elif r.status_code == 204: # No Content
    file = open(thing+".cred","r")
    credentials = json.load(file)
else:
    print("cannot get certificates. Error code " + str(r.status_code)) + ": " + r.text
    exit(1)


device_id = credentials["device_id"]
print("device_id = " + device_id)
# store X509 certficate and key in separate files for the sake of python
file = open(thing+".crt","w") 
file.write(credentials["cert"])
file.close()
file = open(thing+".key","w") 
file.write(credentials["key"])
file.close()

# Read stored configuration
config = "initial config"
try:
    file = open(thing+".cfg","r")
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
            file = open(thing+".cfg","w")
            file.write(config)
            file.close()
            time.sleep(5)
            print("...update done!")
        else:
            print("received old configuration, nothing to do")
        # confirm via report that we have received and handled the configuration
        client.publish("kurbisio/"+device_id+"/twin/reports/configuration", config, qos=0)

client = mqtt.Client(client_id=device_id, clean_session=True)
client.tls_set(ca_certs="../../ca.crt", tls_version=ssl.PROTOCOL_TLSv1_2, certfile=thing+".crt", keyfile=thing+".key")
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost",8883)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
client.loop_forever()
