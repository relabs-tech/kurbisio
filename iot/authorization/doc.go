/*Package authorization provides the REST interface for the core IoT appliance device authorization

The package provides a single endpoint which allows devices to check their authentication status and
to download X509 device certificates to authenticate with the IoT MQTT broker.

The API provides the following REST route:
	GET /authorizations/{equipment_id}

Device authorization is done with a GET request to /authorizations/{equipment_id}. If the device's
authorization_status is "waiting", the endpoint returns a new X509 certificate (as cert and key) as well as the
platform's device_id UUID. The certificate is device specific, e.g. it has the device_id baked into the common name
of the certificate. After that the authorization status is set to "authorized".

Subsequent calls to the endpoint do only return the device_id, i.e. the certificate and the key can only be retrieved once. This
is for security purposes.

Clients should always call this endpoint during startup to validate that their authorization has not been
withdrawn.

Database Requirements

The service assumes that the database manages a resource "device" with an external index "equipment_id" and
a static resource for the "authorization_status", like this:
  {
	"resource": "device",
	"external_indices": ["equipment_id"],
	"static_properties": ["authorization_status"]
  }

*/
package authorization
