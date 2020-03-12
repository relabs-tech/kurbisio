#!/usr/bin/env python
import paho.mqtt.client as mqtt
import ssl
import requests
import os.path
import json

imei = os.path.basename(__file__).split('.')[0]



print("check registration for Device1")
r = requests.get("http://localhost:3000/authorizations/"+imei).json()
device_id = r["device_id"]
print("my device id is " + device_id)
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


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("kurbisio/twin/"+device_id+"/requests/configuration")
    client.subscribe("kurbisio/twin/"+device_id+"/requests/gregory")
    client.publish("kurbisio/twin/"+device_id+"/get", json.dumps(["configuration","gregory"]))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("received data on " + msg.topic + " (" + str(len(msg.payload)) + " bytes) " + str(msg.payload))
    if msg.topic == "kurbisio/twin/"+device_id+"/requests/configuration":
        client.publish("kurbisio/twin/"+device_id+"/reports/configuration", msg.payload, qos=0) 


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
