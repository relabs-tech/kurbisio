/*Package twin provides the REST interface for the core IoT appliance device twin

A device twin is a set of JSON objects, each described with a unique key.

The service always manages two sides for any given key: the request and the report.
The request is an object that gets transfered to the device. The report is the device's
answer to that request.

For working with the device twin, the API provides the following REST routes:

The API provides the following REST routes:
	GET /devices/{device_id}/twin
	GET /devices/{device_id}/twin/{key}
	GET /devices/{device_id}/twin/{key}/request
	PUT /devices/{device_id}/twin/{key}/request
	GET /devices/{device_id}/twin/{key}/report
	PUT /devices/{device_id}/twin/{key}/report

The system keeps track of the time, when a request or report was posted, and returns this
information with GET /devices/{device_id}/twin/{key}. Example:
  curl ..../devices/{device_id}/twin/configuration
  {
	"key": "configuration",
	"request": {
	  "version": "3.2"
	},
	"report": {
	  "version": "3.2"
	},
	"requested_at": "2020-03-24T16:39:49.581168Z",
	"reported_at": "2020-03-24T17:32:49.09863Z"
  }

Multiple equal updates from the device via MQTT do not change the reported_at timestamp. The
reported_at timestamp stores the time when a report was received the first time.


A GET on /devices{device_id}/twin returns a list of twin objects for all available keys.

Database Requirements

The service assumes that the database manages a resource "device". It creates an additional
table "twin" to store the device twin.

*/
package twin
