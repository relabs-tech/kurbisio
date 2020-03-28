/*
Package baas implements a backend-as-a-service

A backend manages a Postgres-SQL database and provides an auto-generated RESTful-API for it.

Configuration

The configuration is done entirely via JSON.

Example:
  {
	"resources": [
	  {
		"resource": "user",
		"external_unique_indices": ["email"]
	  },
	  {
		"resource": "user/profile",
		"single": true
	  },
	  {
		"resource": "device",
		"external_indices": ["equipment_id"]
	  }
	],
	"relations": [
	  {
		"origin": "device",
		"resource": "user/device"
	  }
	]
  }

The example creates one resource "user" with an external unique index "email" and a static property "name".
Typically properties are managed dynamically in an untyped JSON object "properties", but it is possible
to define a list of static properties, mainly to support simpler SQL queries. In case of doubt, do not define
static properties, but keep everything relevant in the dynamic JSON object.

A user has a child resource "profile", which is marked single, i.e. any user can only have one single profile.
Then there is a relation from device to user which creates another child resource "user/device".

This configuration creates the following REST routes:
	GET /users
	POST /users
	GET /users/{user_id}
	PUT /users/{user_id}
	DELETE /users/{user_id}
	GET /users/{user_id}/profile
	PUT /users/{user_id}/profile
	DELETE /users/{user_id}/profile
	GET /devices
	POST /devices
	GET /devices/{device_id}
	PUT /devices/{device_id}
	DELETE /devices/{device_id}
	GET /users/{user_id}/devices
	PUT /users/{user_id}/devices/{device_id} - with empty request body
	GET /users/{user_id}/devices/{device_id}
	DELETE /users/{user_id}/devices/{device_id}


The models look like this:

	User
	{
		"user_id": UUID,
		"properties":  JSON
		"email": STRING
		"created_at": TIMESTAMP
	}

	Profile
	{
		"profile_id": UUID
		"user_id": UUID,
		"properties":  JSON
		"created_at": TIMESTAMP
	}

	Device
	{
		"device_id": UUID,
		"properties":  JSON
		"equipment_id": STRING
		"created_at": TIMESTAMP
	}


We can now create a user with a simple POST:
  curl http://localhost:3000/users -d'{"email":"test@test.com", "properties":{"name":"Jonathan Test"}}'
  {
	"created_at": "2020-03-23T16:01:08.138302Z",
 	"email": "test@test.com",
 	"properties": {
	  "name": "Jonathan Test"
 	},
 	"user_id": "f879572d-ac69-4020-b7f8-a9b3e628fd9d"
  }

We can create a device:
  curl http://localhost:3000/devices -d'{"equipment_id":"12345"}'
  {
 	"created_at": "2020-03-23T16:07:23.57638Z",
	"device_id": "783b3674-34d5-497d-892a-2b48cf99296d",
	"equipment_id": "12345",
 	"properties": {}
  }

And we can assign this device to the test user:
  curl -X PUT http://localhost:3000/users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/devices/783b3674-34d5-497d-892a-2b48cf99296d
  204 No Content

Now we can query the devices of this specific user:
  curl http://localhost:3000/users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/devices
  [
 	{
	  "created_at": "2020-03-23T16:07:23.57638Z",
	  "device_id": "783b3674-34d5-497d-892a-2b48cf99296d",
	  "equipment_id": "12345",
	  "properties": {}
	 }
  ]

This adds a profile to the user, or updates the user's profile:
  curl-X PUT http://localhost:3000/users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/profile -d'{"properties":{"nickname":"jonathan"}}'
  {
 	"created_at": "2020-03-23T16:25:15.738091Z",
 	"profile_id": "9a09030c-516f-4dcd-a2fc-dedad219457d",
 	"properties": {
	  "nickname": "jonathan"
 	},
	 "user_id": "f879572d-ac69-4020-b7f8-a9b3e628fd9d"
  }

Logged-In Routes

In the above example it would also be possible to request "logged_in_routes" for the resource "user":

  	...
	"resources": [
	  {
		"resource": "user",
		"logged_in_routes" : true
		...
	  },
	...

This would create these additional REST routes for the logged-in user:
	GET /user
	PUT /user
	DELETE /user
	GET /user/profile
	PUT /user/profile
	DELETE /user/profile
	GET /user/devices
	PUT /user/devices/{device_id} - with empty request body
	GET /user/devices/{device_id}
	DELETE /user/devices/{device_id}

Effectively, the path segement /user is replaced with /users/{user_id}, where user_id comes from the Authorization
object of the request context. For this to work, you need an authorization middleware which looks at the
authorization bearer token and adds the necessary Authorization object with user_id to the request context.

Dynamic Properties

Every resource has a property "properties", which contains a free-form JSON object. This object is optional during creation and
then defaults to an empty object. Currently it is not possible to put any constraints onto those objects, but future versions
of the backend will support JSON schema validation for those objects.

Static Properties

In the example above, we have extended the user and the device resource with external indices. Likewise it is possible to extend
resource with static string properties, using an array "static_properties". The main purpose of this is to enable easier SQL queries
against generated tables, for example we use it to store the authorization_status for IoT devices. In the regular case, properties
of resource should go into the dynamic JSON object for maximum flexibility.

Sorting and Creation Time

Collections of resources are sorted by the created_at timestamp, with latest first. For additional flexibility, it is possible
to overwrite the timestamp in a POST or PUT request. If you for example import workout activities of a user, you may choose to
use the starte time of each activity as created_at time.

In the future it should also be possible to change the sorting order and to query collection with a specific time-range.

Query Parameters

The GET request on single resources - i.e. not on collections - can be customized with the "children" query parameter. It makes
it possible to add child resources to the response, avoiding unnecessary rest calls. For example. if you want to retrieve
a specific user, the user's profile and the user's devices, you can do all that with a single request to
	GET /user?children=profile,devices
or
	GET /user?children=profile&children=devices

The GET request on collections can be customized with any of the static properties or external indices. In our example,
the resource "user" has an external index "email", hence we can query all users for a specific email with
	GET /users?email=test@test.com

Notifications

The backend supports notifications via the WithNotifier() modifier and the Notifier interface.

Authorization

The backend mangages role based access control to its resource. See MustNewJwtMiddleware. TBD.

Client interface

The backend provides a client interface, which enables a convient way to invoke any of the generated
REST functions from within the same go process.

Registry

The backend provides a simple JSON registry for persisted values, see Registry().
The data is stored in a system table "_registry_" in the backend's database schema.


*/
package baas
