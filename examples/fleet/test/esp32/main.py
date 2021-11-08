import machine
import ssd1306
import utime
import ujson as json
import os
import urequests
import ssl
import network
import errno
from umqtt.simple import MQTTClient

# TO BE UPDATED
WIFI_SSID = ""
WIFI_PASSWORD = ""
KURBISIO_HOST = "192.168.1.192"  # This is the IP the computer running the backend


# The following is
KURBISIO_PORT = 3000
KURBISIO_THING_NAME = "Device1"
KURBISIO_SHARED_SECRET = "fleet-thing-secret"


class HeltecWifiKit():
    def __init__(self, thing_name, kurbisio_host, kurbisio_port=3000):
        self.display = None
        self.mpu9250 = None
        self.fusion = None
        self.kurbisio_host = kurbisio_host
        self.kurbisio_port = kurbisio_port
        self.thing_name = thing_name
        self.credentials = {}
        self.mqtt_client = None
        self.init_display()
        try:
            with open(self.thing_name+".cfg", "r") as f:
                self.config = json.load(f)
        except OSError:
            self.config = {}
        self.last_display_message = ""

    def init_display(self):
        i2c = machine.I2C(scl=machine.Pin(15), sda=machine.Pin(4))
        pin16 = machine.Pin(16, machine.Pin.OUT)
        pin16.value(1)
        self.display = ssd1306.SSD1306_I2C(128, 64, i2c)

    def display_message(self, message):
        self.last_display_message = message
        self.render_screen()

    def render_screen(self):
        self.display.fill(0)
        self.display.text(self.last_display_message, 0, 0)
        y = 10
        for k, v in self.config.items():
            self.display.text("{}: {}".format(k, v), 0, y)
            y += 10
        self.display.show()

    def connect_wifi(self, ssid, pwd):
        wlan = network.WLAN(network.STA_IF)
        wlan.active(True)
        if not wlan.isconnected():
            wlan.connect(ssid, pwd)
            while not wlan.isconnected():
                pass

    def on_config_received(self, new_config):
        if self.config != new_config:
            print("received new configuration, updating...")
            self.config = new_config
            with open(self.thing_name + ".cfg", "w") as f:
                json.dump(self.config, f)
            device_id = self.credentials["device_id"]
            self.mqtt_client.publish("kurbisio/" + device_id + "/twin/reports/configuration",
                                     json.dumps(self.config), qos=0)
            self.render_screen()

    def fetch_credentials(self, shared_secret):
        try:
            url = ''.join(["http://", self.kurbisio_host, ":",
                           str(self.kurbisio_port), "/credentials"])
            r = urequests.get(url, headers={
                "Kurbisio-Thing-Key": shared_secret,
                "Kurbisio-Thing-Identifier": self.thing_name})
        except OSError as e:
            if e.args[0] not in (errno.ECONNRESET, errno.ECONNABORTED):
                raise
            return False

        if r.status_code == 200:  # OK
            with open(self.thing_name+".cred", "w") as f:
                f.write(r.text)
            self.credentials = r.json()
            print("received new credentials")
        elif r.status_code == 204:  # No Content
            with open(self.thing_name+".cred", "r") as f:
                self.credentials = json.load(f)
            print("reused existing credentials")
        else:
            print("cannot get credentials. Error code " + str(r.status_code))
            return False
        print("Got device_id = " + self.credentials["device_id"])
        return True

    def on_message(self, topic, msg):
        print("received data on " + str(topic) + ": " + str(msg))
        if topic == b"kurbisio/" + self.credentials["device_id"] + "/twin/requests/configuration":
            self.on_config_received(json.loads(str(msg.decode("utf-8"))))

    def start_mqtt(self):
        device_id = self.credentials["device_id"]
        self.mqtt_client = MQTTClient(client_id=device_id, server=self.kurbisio_host, ssl=True,
                                      ssl_params={
                                          "cert": self.credentials["cert"],
                                          "key": self.credentials["key"]
                                      })
        self.mqtt_client.connect()
        self.mqtt_client.set_callback(self.on_message)
        self.mqtt_client.subscribe(b"kurbisio/" + device_id + "/twin/requests/configuration")
        self.mqtt_client.publish(b"kurbisio/" + device_id + "/twin/get",
                                 json.dumps(["configuration"]))

        while True:
            self.mqtt_client.wait_msg()


k = HeltecWifiKit(KURBISIO_THING_NAME, KURBISIO_HOST, KURBISIO_PORT)
k.display_message("Connecting to Wifi")
k.connect_wifi(WIFI_SSID, WIFI_PASSWORD)
k.display_message("Connected to Wifi")
while not k.fetch_credentials(KURBISIO_SHARED_SECRET):
    k.display_message("Waiting for Kurbisio...")
    utime.sleep(1)
k.display_message("Connected")
k.start_mqtt()
k.display_message("Exited")
