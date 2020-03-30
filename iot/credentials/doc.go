/*Package credentials implements a REST interface which provides device credentials to things

The package generates a single endpoint where things can download credentials which allow them
to authenticate as a device. The credentials include the thing's device id, device-specific X.509
certificates to authenticate with the IoT MQTT broker, and a bearer token to be used with REST APIs.

The API provides the following REST route:
	GET /certificates

A thing must authenticate by providing a secret key as header "Kurbisio-Thing-Key" and its own
thing identifier as header "Kurbisio-Thing-Identifier".

The returned credentials are
	device_id:	the device id for this thing
	cert:		the X.509 certificate for the MQTT client
	key: 		the X.509 private key for the MQTT client
	token: 		a bearer token for HTTPS requests

Note that credentials are returned only once for security purposes. Subsequent requests by the same thing result in
in 204 No Content or - in case the thing's device authorization has been withdrawn -  401 Unauthorized.
Clients can always call this endpoint during startup to validate that their authorization has not been
withdrawn.

Database Requirements

The service assumes that the database manages a resource "device", with an external index "thing" and a static
resource "provisioning_status", like this:
  {
	"resource": "device",
	"external_indices": ["thing"],
	"static_properties": ["provisioning_status"]
  }

Credentials can be downloaded if and only if the provisioning status is "waiting". After a successful
download, the status is automatically set to "provisioned".

*/
package credentials
