# The Fleet Use Case
This folder contains the fleet management example. The fleet use case allows to define fleets of
devices, devices and users. Each device is assocaited to a fleet. Users can have multiple devices
associated to them.

Two examples of device's implementations are provided.
- A pure python example which can be run on a computer or a Raspberry Pi. 
- A micropython example which can be run on a ESP32 chipset

# Starting the server side

1. Start database:

`docker run --rm --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres`

1. Start streaming server:

`docker run -p 4223:4223 -p 8223:8223 nats-streaming nats-streaming-server -p 4223 -m 8223`

1. Start the fleet service:

From the `services/fleet` folder run the following command:

`POSTGRES="host=localhost port=5432 user=postgres password=docker dbname=postgres sslmode=disable" go run fleet.go`

1. Create users and services:

From the `services/fleet/test/backend` and in a different shell than the previous command, run:
   1. `npm install`
   1. `npm start`

This will create the `Matthias Ettrich` user as well as one device.

# Using the Python based device

This is a pure python example which can be run on a computer or a Raspberry Pi for instance.

From the `services/fleet/test/py-device` run:

`./Device1.py`

This will download the device's certificate, register with the backend and request the MQTT
`configuration` from the twin service.


# Using an ESP32 with micropython:
An ESP32 based chipset can be used to connect to the fleet backend. This tutorial uses micropython
to program a Heltec ESP32 Wifi Kit.

The following commands should be run from `services/fleet/test/esp32` run:

## Flash micropython on the ESP32
`esptool` and `adafruit-ampy` are convenient tools to operate the ESP32 from a computer. They can
be installed with the following commands:

```
pip install esptool
pip install adafruit-ampy
```

Next connect the ESP32 to a USB port on your computer. A TTY or COM port will be created. Depending
on the OS this will have different name. On MacOS, this may be called `/dev/cu.SLAB_USBtoUART`

The flash of the ESP32 should be erased before loading the micropython based OS. Execute the
following command to erase the flash. Note that `/dev/cu.SLAB_USBtoUART` must be replaced with the
name of the relevant TTY or com port.

`esptool.py --chip esp32 --port /dev/tty.SLAB_USBtoUART erase_flash`

This will produce the following output:
```
Serial port /dev/cu.SLAB_USBtoUART
Connecting........_
Chip is ESP32D0WDQ6 (revision 1)
Features: WiFi, BT, Dual Core, 240MHz, VRef calibration in efuse, Coding Scheme None
Crystal is 26MHz
MAC: 24:6f:28:22:43:e8
Uploading stub...
Running stub...
Stub running...
Erasing flash (this may take a while)...
```

Then download the latest microptython port for the ESP32 from https://micropython.org/download#esp32
I have used the v1.12 version. The below command downloads this specific version:

`curl -O https://micropython.org/resources/firmware/esp32-idf3-20191220-v1.12.bin`

Then, flash this binary to the ESP32:

`esptool.py --chip esp32 --port /dev/tty.SLAB_USBtoUART write_flash -z 0x1000 esp32-idf3-20191220-v1.12.bin `

Download and load a display driver for the OLED display of the Heltec board:

```
curl -O https://raw.githubusercontent.com/adafruit/micropython-adafruit-ssd1306/master/ssd1306.py
ampy --port /dev/tty.SLAB_USBtoUART --baud 115200 put ssd1306.py
```

Edit the main.py file to update the following three variable to match your environment
WIFI_SSID = ""
WIFI_PASSWORD = ""
KURBISIO_HOST = "192.168.1.192"  # This is the IP the computer running the backend

Execute the main program on the board:
`ampy --port /dev/tty.SLAB_USBtoUART run -n main.py`

Alternatively, you can also push to main.py to the board and it will be executed each time the board boots:
`ampy --port /dev/tty.SLAB_USBtoUART put main.py`

The ESP32 will first try to connect to Wifi, then try to connect to the backend, retrieve the
`configuration` from the twin and display the configuration on the small display.

You can update the `configuration` using the twin REST API. You need to replace
`b8647ee2-e410-4748-873d-7ab37fea235f` with the device ID that was allocated. This can be retrieved
from the backend's logs.

```
curl -X PUT http://localhost:3000/devices/b8647ee2-e410-4748-873d-7ab37fea235f/twin/configuration/request 
-d '{"version":"3.0", "test":"value"}'
```

You should see on the display of the device that the new configuration is displayed.

# How to regenerate certificates
Certificate and keys are provided here. In case you need to regenerate them, the below instructions
can be used.

## Generate the CA Key and Certificate
openssl req -x509 -sha256 -newkey rsa:4096 -keyout ca.key -out ca.crt -days 356 -nodes -subj '/CN=Kurbisio Cert Authority'
## Generate the Server Key, and Certificate and Sign with the CA Certificate
openssl req -new -newkey rsa:4096 -keyout server.key -out server.csr -nodes -subj '/CN=kurbis.io'
openssl x509 -req -sha256 -days 365 -in server.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out server.crt
## Generate the Client Key, and Certificate and Sign with the CA Certificate (this is done by service itself)
openssl req -new -newkey rsa:4096 -keyout client.key -out client.csr -nodes -subj '/CN={device_id}'
openssl x509 -req -sha256 -days 365 -in client.csr -CA ca.crt -CAkey ca.key -set_serial 02 -out client.crt
