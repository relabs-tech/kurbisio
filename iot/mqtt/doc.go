/*Package mqtt provides the IoT broker with device twin support

A device twin is a set of JSON objects, each described with a unique key.

For working with the device twin, the broker supports the following
MQTT topics:

	kurbisio/{device_id}/twin/requests/{key}
	kurbisio/{device_id}/twin/reports/{key}
	kurbisio/{device_id}/twin/get

Requests and Reports

The service always manages two sides for any given key: the request and the report.
The request is an object that gets transfered to the device. The report is the device's
answer to that request.

For example, a system could request a software version by publishing a message
  {"version": "2.0"}
to /twin/requests/software_version.

The device would subscribe to this channel, receive the request, update its software and
then report the result by posting
  {"version": "2.0"}
to /twin/reports/software_version.

The REST API for twin also contains the time stamps when a request or a report was published.

Retrieving Requests

A connected device will immediately receive requests whenever the device twin is updated via
the REST API. In order to receive the latest requests after establishing the connection, the
device should publish a message to /twin/get with a JSON list of keys the device has subscribed
to. In our example the device would publish
  ["software_version"]
to /twin/get.

Database Requirements

The broker assumes that the database manages a resource "device". It creates an additional
table "twin" to store the device twin.
*/
package mqtt
