/*
Package backend implements the configurable backend

A backend manages a Postgres-SQL database and provides an auto-generated RESTful-API for it.

Configuration

The configuration is done entirely via JSON. It consists of collections, singletons, blobs
and relations

Example:
  {
	"collections": [
	  {
		"resource": "user",
		"external_index": "identity"
	  },
	  {
		"resource": "device",
		"external_index": "thing"
	  }
	],
	"singletons": [
	  {
		"resource": "user/profile",
	  }
	],
	"relations": [
	  {
		"origin": "device",
		"resource": "user/device"
	  }
	]
  }

The example creates one resource "user" with an external unique index "identity" and a static property "name".
Typically properties are managed dynamically in an untyped JSON object "properties", but it is possible
to define a list of static properties, mainly to support simpler SQL queries. In case of doubt, do not define
static properties, but keep everything relevant in the dynamic JSON object.

A user has a child resource "user/profile", which is declared as a singleton, i.e. every user can only have one single profile.
Finally there is a relation from device to user which creates another child resource "user/device".

This configuration creates the following REST routes:
	GET /users
	POST /users
	GET /users/{user_id}
	PUT /users/{user_id}
	PATCH /users/{user_id}
	DELETE /users/{user_id}
	GET /users/{user_id}/profile
	PUT /users/{user_id}/profile
	PATCH /users/{user_id}/profile
	DELETE /users/{user_id}/profile
	GET /devices
	POST /devices
	GET /devices/{device_id}
	PUT /devices/{device_id}
	PATCH /devices/{device_id}
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
		"identity": STRING
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
		"thing": STRING
		"created_at": TIMESTAMP
	}


We can now create a user with a simple POST:
  curl http://localhost:3000/users -d'{"email":"test@test.com", "properties":{"name":"Jonathan Test"}}'
  {
	"created_at": "2020-03-23T16:01:08.138302Z",
 	"identity": "test@test.com",
 	"properties": {
	  "name": "Jonathan Test"
 	},
 	"user_id": "f879572d-ac69-4020-b7f8-a9b3e628fd9d"
  }

We can create a device:
  curl http://localhost:3000/devices -d'{"thing":"12345"}'
  {
 	"created_at": "2020-03-23T16:07:23.57638Z",
	"device_id": "783b3674-34d5-497d-892a-2b48cf99296d",
	"thing": "12345",
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
	  "thing": "12345",
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
	"collections": [
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

In the example above, we have extended the user and the device collections with an external index. Likewise it is possible to extend
resource with list of static string properties, using an array "static_properties". The main purpose of this is to enable easier SQL
queries against generated tables, for example we use it to store the authorization_status for IoT devices. In the regular case, properties
of resource should go into the dynamic JSON object for maximum flexibility.

Static properties can be made searchable by adding them to the "searchable_properties" array instead. This activates a query
parameter in the collection get route with the name of the property. See the chapter on query parameters and pagination below.

Sorting and Creation Time

Collections of resources are sorted by the created_at timestamp, with latest first. For additional flexibility, it is possible
to overwrite the timestamp in a POST or PUT request. If you for example import workout activities of a user, you may choose to
use the start time of each activity as created_at time.

The creation time has one more useful side effect: Since the default timestamp for the ?from query parameter is
"0001-01-01 00:00:00 +0000 UTC" (which happens to be the golang zero time), any resource that is created with
an earlier timestamp ends up in a quasi hidden state. While it remains accessible with a fully qualified access path, it will not be
listed in collections. This makes it possible to create a resource with child resources and relations, and only make it
visible to applications when the entire set it ready.

Query Parameters and Pagination

The GET request on single resources - i.e. not on entire collections - can be customized with the "children" query parameter.
It makes it possible to add child resources to the response, avoiding unnecessary rest calls. For example. if you want to retrieve
a specific user, the user's profile and the user's devices, you can do all that with a single request to
	GET /user?children=profile,devices
or
	GET /user?children=profile&children=devices

The GET request on collections can be customized with any of the searchable properties or an external index.
In our example, the resource "user" has an external index "identity", hence we can query all users for a specific identity with
	GET /users?identity=test@test.com

The system supports pagination and filtering of responses by creation time.
	  ?limit=n  sets a page limit of n items
	  ?page=n   selects page number n
	  ?from=t   selects items created at or after the timestamp t
	  ?until=t  selects items created up until and including the timestamp t. The default is "0001-01-01 00:00:00 +0000 UTC".

The response carries the following custom headers for pagination:
	  "Pagination-Limit"        the page limit
	  "Pagination-Total-Count"  the total number of items in the collection
	  "Pagination-Page-Count"   the total number of pages in the collection
	  "Pagination-Current-Page" the currently selected page

The maximum allowed limit is 100, which is also the default limit. Combining pagination with the until-filter
avoids page drift. A well-behaving application would get the first page without any filter, and then use the created_at
value of the first received item as until-parameter for querying pages further down.

Note: Due to some peculiarities of Postgres, the total count and the page count are always zero
if the requested page is out of range.

Primary Resource Identifier

The primary resource identifier is not mandatory when creating resources. If the creation request (POST or PUT) contains
no identifier or a null identifier, then the system creates a new unique UUID for it. Yet it is possible to specify
a primary identifier in the request, which will be honored by the system. This feature - and the choice of UUID for
primary identifiers - makes it possible to easily transfer data between different databases.

Notifications

The backend supports notifications through the Notifier interface specified at construction time.
TBD describe notifications in configuration JSON

Authorization

The backend mangages role based access control to its resource. See access package for details. TBD.

Blobs

TODO describe blobs. Additional property: MaxAgeCache, which goes into the cache-control header as max-time.
It's the time to live in the cache in seconds. The default is 31536000 (which is one year worth of seconds).
Set it to -1 if you want no cache.



*/
package backend
