// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

/*
Package backend implements the configurable backend

A backend manages a Postgres-SQL database and provides an auto-generated RESTful-API for it.

# Configuration

The configuration is done entirely via JSON. It consists of collections, singletons, blobs
and relations

Example:

	  {
		"collections": [
		  {
			"resource": "user",
			"external_index": "identity"
			"schema_id": "https://backend.com/schemas/user.json"
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
			"left": "device",
			"right": "user"
		  }
		]
	  }

The example creates one resource "user" with an external unique index "identity".

A user has a child resource "user/profile", which is declared as a singleton, i.e. every user can only have one single profile.
Hence a profile does not have an id of its own, but uses the user_id as its primary identifier, and there
is a convenient singular resource accessor for a user's profile.

Finally there is a relation from device to user which creates two more virtual child resources "user/device" and "device/user".

This configuration creates the following REST routes:

	/users GET,POST,PUT,PATCH
	/users/{user_id} GET,PUT,PATCH,DELETE
	/devices GET,POST,PUT,PATCH
	/devices/{device_id} GET,PUT,PATCH,DELETE
	/users/{user_id}/devices GET
	/users/{user_id}/devices/{device_id} GET,PUT,DELETE
	/devices/{device_id}/users GET
	/devices/{device_id}/users/{user_id} GET,PUT,DELETE
	/users/{user_id}/profile GET,PUT,PATCH,DELETE
	/users/{user_id}/profiles GET,POST,PUT,PATCH
	/users/{user_id}/profiles/{user_id} GET,PUT,PATCH,DELETE

The models look like this:

	User
	{
		"user_id": UUID,
		"identity": STRING
		"timestamp": TIMESTAMP
		"revision": INTEGER
		...
	}

	Profile
	{
		"profile_id": UUID
		"user_id": UUID,
		"timestamp": TIMESTAMP
		"revision": INTEGER
		...
	}

	Device
	{
		"device_id": UUID,
		"thing": STRING
		"timestamp": TIMESTAMP
		"revision": INTEGER
		...
	}

We can now create a user with a simple POST:

	  curl http://localhost:3000/users -d'{"identity":"test@test.com", "name":"Jonathan Test"}'
	  {
		"timestamp": "2020-03-23T16:01:08.138302Z",
	 	"identity": "test@test.com",
		"name": "Jonathan Test",
	 	"user_id": "f879572d-ac69-4020-b7f8-a9b3e628fd9d"
	  }

We can create a device:

	  curl http://localhost:3000/devices -d'{"thing":"12345"}'
	  {
	 	"timestamp": "2020-03-23T16:07:23.57638Z",
		"device_id": "783b3674-34d5-497d-892a-2b48cf99296d",
		"thing": "12345"
	  }

And we can assign this device to the test user:

	curl -X PUT http://localhost:3000/users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/devices/783b3674-34d5-497d-892a-2b48cf99296d
	204 No Content

Now we can query the devices of this specific user:

	  curl http://localhost:3000/users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/devices
	  [
	 	{
		  "timestamp": "2020-03-23T16:07:23.57638Z",
		  "device_id": "783b3674-34d5-497d-892a-2b48cf99296d",
		  "thing": "12345"
		 }
	  ]

This adds a profile to the user, or updates the user's profile:

	  curl-X PUT http://localhost:3000/users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/profile -d'{"nickname":"jonathan"}'
	  {
	 	"timestamp": "2020-03-23T16:25:15.738091Z",
	 	"profile_id": "9a09030c-516f-4dcd-a2fc-dedad219457d",
		"nickname": "jonathan",
		"user_id": "f879572d-ac69-4020-b7f8-a9b3e628fd9d"
	  }

# Shortcut Routes

The above example can be made even more user friendly, by adding shortcut routes for the authenticated user. Say we
have a role "userrole" which contains a selector for a user resource. Then we can declare a shortcut with

	  	...
		"shortcuts": [
		  {
			"shortcut": "user",
			"target" : "user",
			"roles": ["userrole"]
		  }
		...

This creates additional REST routes where every path segment /users/{user_id} is replaced with the shortcut /user for all
generated routes. For example, instead of querying a user's devices with users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/devices
you would simply query /user/devices.

# Revisions

Every item has an integer property "revision", which is incremented every time the item is updated. Revisions can be
used to make updates safe in systems with multiple concurrent writers. If a PUT or PATCH request contains a
non-zero revision number which does not match the item's current revision, then the request is discarded and
the conflicting newer version of the object is returned with an error status (409 - Conflict).
A PUT or PATCH request with a revision of zero, or no revision at all, will not be checked for possible conflicts.

# Wildcard Queries

You can replace any id in a path segment with the keyword "all". For example, if some administrators wants
to retrieve all profiles from all users, they would query

	GET /users/all/profiles

# Schema Validation

Every resource by default is essentially a free-form JSON object. This gives a high degree of flexibility, but is prone to errors.
Therefore you can define a JSON schema ID for any Singleton or Collection resource. If the "schema_id" is
defined, any attempt to PUT, POST or PATCH  this resource will be validated against this schema.
If validation fails, error 400 will be returned.

# Default Properties

Any Singleton or Collection resource can have an additional property "default", which defines default properties for
all instances. Default properties are automatically added whenever objects are created or updated in the database.
In addition, they are also added when older versions of objects are read from the database. Default properties
are especially useful in combination with schema validation, as they make it possible to add new required properties
without having to migrate all existing objects in the database.

# Static Properties

In the example above, we have extended the user and the device collections with an external index. Likewise it is possible to extend
resource with list of static string properties, using an array "static_properties". Static properties (searchable or not) have the
advantage, that they can be updated must faster than any other dynamic property. If the user resource from above had a static
property "name", you could update that name quickly with

	POST /user/{user_id}/name/{new_name}

It is only in rare occasions when you actually need this. In the regular case, properties of a resource should not need to be
declared static, and property updates should be done with a standard PATCH request, returning the fully patched object.

There is one more application for static properties: Since they have their own underlying SQL column, they also enable easier
SQL queries against generated tables for other services with direct acccess to the database.  For example, we use a static property
to store the provisioning_status for IoT devices.

Static properties can be made searchable by adding them to the "searchable_properties" array instead. This activates a filter
in the collection get route with the name of the property. See the chapter on query parameters and pagination below.

# Sorting and Timestamp

Collections of resources are sorted by the timestamp, with latest first. For additional flexibility, it is possible
to overwrite the timestamp in a POST or PUT request. If you for example import workout activities of a user, you may choose to
use the start time of each activity as timestamp.

# Query Parameters and Pagination

The GET request on single resources - i.e. not on entire collections - can be customized with the "children" query parameter.
It makes it possible to add child resources to the response, avoiding unnecessary rest calls. For example. if you want to retrieve
a specific user, the user's profile and the user's devices, you can do all that with a single request to

	GET /user?children=profile,devices

or

	GET /user?children=profile&children=devices

By using the paramter nointercept=true, it is possible to supress any interceptors and return the latest version of the document stored.

The GET request on collections can be customized with any of the searchable properties, an external index, the ids of
the resources or the first layer of properties of the json document as a filter. It is possible to search for equality of to search
a pattern.

# Searching and Filtering

Collections support two different operators for searching and filtering: search and filter. The operator "search" is guaranteed to be fast, it only
works on external indices or explicitly marked searchable properties. If you try to search for resources by a different property it
will flag a bad request error. The operator "filter" will try to use database indices when available, but it will also filter based on
JSON properties.

Searching for equality:
In our example, the resource "user" has an external index "identity", hence we can query all users for a specific identity with

	GET /users?filter=identity=test@test.com

Searching pattern:
Searching for pattern is done using the `~` character instead of `=`. Pattern are written using SQL LIKE format.
% represents zero, one, or multiple characters
_ represents one, single character

	GET /users?filter=identity~%@test.com
	returns all users with an email which ends with @test.com

If you specify multiple filters, they filter on top of each other (i.e. with logical AND).

Filters can be combined with the wildcard 'all' keyword. For instance, it is possible to get all the devices of a user by filtering
on the user_id property

	GET /users/all/devices?filter=user_id=f879572d-ac69-4020-b7f8-a9b3e628fd9d
		This is equivalent to using the following, but may be more convenient to write in some cases.
	GET users/f879572d-ac69-4020-b7f8-a9b3e628fd9d/devices

The system supports pagination and filtering of responses by creation time. Two pagination methods are available:

## Traditional Page-based Pagination (only when page parameter is specified, ?page=1 for first page)

	?order=[asc|desc]  sets the sorting order to be descending (newest first, the default) or ascending (oldest first)
	?limit=n  sets a page limit of n items
	?page=n   selects page number n. The first page is page 1
	?from=t   selects items created at or after the timestamp t
	?until=t  selects items created up until and including the timestamp t. The default is "0001-01-01 00:00:00 +0000 UTC".
	Timestamps must be formatted following RFC3339 (https://tools.ietf.org/html/rfc3339).

## Cursor-based Pagination (Default)

	?order=[asc|desc]  sets the sorting order to be descending (newest first, the default) or ascending (oldest first)
	?limit=n  sets a page limit of n items
	?next_token=token  specifies the cursor token for retrieving the next page of results
	?from=t   selects items created at or after the timestamp t
	?until=t  selects items created up until and including the timestamp t. The default is "0001-01-01 00:00:00 +0000 UTC".
	Timestamps must be formatted following RFC3339 (https://tools.ietf.org/html/rfc3339).

Cursor-based pagination is used by default when no page parameter is specified. The page and next_token parameters are mutually exclusive.

The response carries the following custom headers for pagination:

For traditional page-based pagination (deprecated):

	"Pagination-Limit"        the page limit
	"Pagination-Page-Count"   the total number of pages in the collection
	"Pagination-Current-Page" the currently selected page
	"Pagination-Total-Count"  the total number of items in the collection

For cursor-based pagination:

	"Pagination-Limit"        the page limit
	"Pagination-Next-Token"   the cursor token for the next page (only present if more data is available)

The maximum allowed limit is 100, which is also the default limit.

For collections it is possible to only retrieve meta data, by specifying the ?onlymeta=true query parameter. Meta data are
all defining identifiers, the timestamp and each object's revision number.

# Primary Resource Identifier

The primary resource identifier is not mandatory when creating resources. If the creation request (POST or PUT) contains
no identifier or a null identifier, then the system creates a new unique UUID for it. Yet it is possible to specify
a primary identifier in the request, which will be honored by the system. This feature - and the choice of UUID for
primary identifiers - makes it possible to easily transfer data between different databases.

# Notifications

The backend supports notifications through the Notifier interface specified at construction time.

# Relations

The example demonstrated a relation between "user" and "device", which created two additional resources "user/device" and
"device/user". Relations also work between different child resources, for example between "fleet/user" and "fleet/device",
as long as both resources have a compatible base (in this case "fleet"). Furthermore relations are transient. Say you
have actual resources "device" and "fleet", and a relation between them, which creates a virtual resource "fleet/device".
In this case you can also have a relation between "fleet/user" and "fleet/device", leading to the two additional
resources "fleet/user/device" and "fleet/device/user".

Relations support separate permits for the left and the right resource, called "left_permits" and "right_permits".
"left_permits" applies to the left/right relations.
"right_permits" applies to the right/left relations.

Examples:
  - to read left/{left_id}/right/{right_id}, one needs to have the "read" permission on the left_permit.
  - to read right/{right_id}/left/{left_id}, one needs to have the "read" permission on the right_permit.
  - to create left/{left_id}/right/{right_id}, one needs to have the "create" permission on the left_permit and read permission on the right resource
  - to create right/{right_id}/left/{left_id}, one needs to have the "create" permission on the right_permit and read permission on the left resource
  - to list left/{left_id}/rights, one needs to have the "list" permission on the left_permit.
  - to list right/{right_id}/lefts, one needs to have the "list" permission on the right_permit.
  - to delete left/{left_id}/right/{right_id}, one needs to have the "delete" permission on the left_permit.
  - to delete right/{right_id}/left/{left_id}, one needs to have the "delete" permission on the right_permit.
  - the update permission is not used

For each relation, the number of related resources for one other resource is currently limited by 1000. In the above
example, one fleet can have up to 1000 users and devices, and each user then can be assigned to 1000 devices max.

Relations support an extra query parameter "?idonly=true", which returns only the list of ids as opposed to full objects.
If you furthermore specify "withtimestamp=true", you will receice both the ids and the timestamp when this relation was
established.

Relations can also be given an explicit Resource name just like any other collection, which allows multiple different
relations from the the same resource types. The resource name then becomes a prefix to access the relation.

# Blobs

Blobs are collections of binary resources. They will be served to the client as-is. You can use blobs
for example to manage a collection of images like this:

	  	"blobs": [
		  {
			"resource": "image",
			"static_properties" : ["content_type"]
		  }
		]

This configuration creates the following routes:

	GET /images  - returns meta data of all images as JSON
	POST /images
	GET /images/{image_id}
	DELETE /images/{image_id}

All static properties, searchable properties and external indices of a blob are passed as canonical headers.
The property "content_type" hence becomes a header "Content-Type". All other properties are transferred as the
header "Kurbisio-Meta-Data".

Blobs are immutable by default, which means they can be optimally cached. If you need blobs that can be
updated, for example a profile image, you get declare them mutable like this:

	  	"blobs": [
		  {
			"resource": "image",
			"static_properties" : ["content_type"],
			"mutable": true,
			"max_age_cache": 3600
		  }
		]

This creates the extra route

	PUT /images/{image_id}

In the example we have also set the "max_age_cache" property to 3600 seconds, which is one hour. The default
for mutable blobs is no caching at all. Mutable blobs also support Etag and If-Non-Match out-of-the-box,
which allows clients to check for updates quickly without re-downloading the entire blob. See section
on If-None-Match and Etag below.

# Authorization

If AuthorizationEnabled is set to true, the backend supports role based access control to its resources.
By default, only the "admin" role has a permit to access resources. A permit object for each resource
authorizes specific roles to execute specific operations. The different operations are: "create", "read", "update",
"delete", "list" and "clear". The "list"-operation is the retrieval of the entire collection, "clear" deletes the entire
collection.

"admin viewer" also has right to access all resources in read only mode. Only read and list operations are permitted.

For example, you want to declare a resource "picture", which is a child-resource of "user". Now you want to
give each user permission to create, read and delete their own pictures, but only their own pictures. You
declare a role for a user - in this case "userrole" - and specify the resource like this:

	{
		"resource": "user/picture",
		"permits": [
			{
				"role": "userrole",
				"operations": [
					"create",
					"read",
					"update",
					"delete",
					"list",
					"clear"
				],
				"selectors": [
					"user"
				]
			}
		]
	}

The selector basically states that the authorization object must contain a concrete user_id, and
that any of the operations is only permitted for this user_id.

Now users want to be able to share links to their pictures. The public should be able to read them,
but they should not be able to list all picture, nor to create new ones nor delete them.
You can achieve this by issueing another permit for the "public" role:

	"permits": [
		...
		{
			"role": "public",
			"operations": [
				"read"
			]
		}
	]

There are three special roles in the system: The "admin" role who has permission to do everything.
The "admin viewer" role has permission to read and list everything, but not modify or create.
The "public" role, which is assumed by every non-authorized request. And finally the "everybody" role,
which is a placeholder for any other role in the system but "public".

You can easily check the authorization state of any token, by doing a GET request to

	/authorization

which will return the authorization state for the authenticated requester as JSON object.

Singletons conceptually always exist, i.e. they can be updated and patched with a permission for
"update", even if there is no object in the database yet.

# If-None-Match and Etag

All GET requests are served with Etag and obey the If-None-Match request. This allows clients to check
whether new data is available without downloading and comparing the entire response. If a client puts
the received Etag of a request into the If-None-Match header of a subsequent request, then the system will
simply response to that subsequent with a 304 Not Modified in case the resource was not changed. In case
the resource was changed, the request will be answered as usual.

# Externally stored data

Collections allow to store a file with each individual collection item. Unlike blobs which should
remain of reasonable size to preserve the performance of the database, a file associated to a collection
item can be large since it is stored outside of the database.
The storage backend can be either an AWS S3 bucket or a local file system. Selecting the storage backend is
done at startup time using <TODO ADD DETAILS>

Local storage is most likely to be used only for testing purpose to avoid the need for an internet connection
and an S3 bucket. Using local filesystem implementation is not intended to be used at scale since it has not been
implemented with performance and scalability as requirements.

To enable file storage for a resource, set property "with_companion_file" to true in the resource configuration.

Example:

	{
		"collections": [
		  {
			"resource": "release",
			"schema_id": "https://backend.com/schemas/release.json"
		  },
		  {
			"resource": "release/artefact",
			"with_companion_file": true,
			companion_presigned_url_validity: 3600
		  }
		],
		"singletons": [
		],
		"relations": [
		]
	  }

In the above example, the release/artefact resource will have the possibility to store a file together with this resource.
Accessing this collection will add to the returned object two string properties `companion_download_url`,
`companion_upload_url`.

They will be populated based on the type of request:
GET adds `download_url`
POST adds `upload_url`
PUT adds `upload_url`
LIST no extra field. If flag `with_companion_urls=true` is set, `download_url` are provided for each item

As their name suggest, the `companion_download_url` and `companion_upload_url`allow to respectively download and upload data.
As a result, uploading and downloading file is a two-steps operation. First the download URL is fetched, then
the URL is used to fetch data. The URL are pre-signed URL with a validity time which is defined in the url itself.

It is possible to define the validity duration of the pre-signed URL in the configuration using the `companion_presigned_url_validity`
key which defines the duration in seconds for which the URL will be valid

# Deleting a resource also delete the associated companion file if it exist

# Audit Logging

The backend supports comprehensive audit logging for tracking operations performed on collections and blobs.
Audit logs capture important information about who performed what action, when, and from where.

## Configuration

Audit logging is configured per resource using the `audit_logs` array in collection or blob configurations.
You can specify which operations to audit by including them in this array.

Available audit log operations:
- `create` - Log when resources are created
- `read` - Log when resources are accessed/read
- `update` - Log when resources are modified
- `delete` - Log when individual resources are deleted
- `clear` - Log when entire collections are cleared

Example configuration for a user collection with full audit logging:

	{
		"collections": [
			{
				"resource": "user",
				"external_index": "email",
				"audit_logs": ["create", "read", "update", "delete", "clear"]
			}
		]
	}

Example configuration for a blob with selective audit logging:

	{
		"blobs": [
			{
				"resource": "document",
				"static_properties": ["content_type"],
				"audit_logs": ["create", "delete"]
			}
		]
	}

## Log Format

Audit logs are written to the application logger using structured logging with the prefix `[AuditLog]`.
The format varies slightly depending on the operation:

For create/update operations:

	[AuditLog] Create user from IP: 192.168.1.100, body: {"user_id":"...","email":"user@example.com"}

For read/delete operations:

	[AuditLog] Read user from IP: 192.168.1.100, path: /users/12345678-1234-1234-1234-123456789abc

For clear operations:

	[AuditLog] Clear user from IP: 192.168.1.100, path: /users?filter=active=false

## IP Address Tracking

Audit logs automatically capture the client's IP address for each operation. The system
handles different network configurations:

- **Direct connections**: Uses the client's actual IP address from the request
- **Proxy/Load balancer scenarios**: Extracts the original client IP from the `X-Forwarded-For` header

# Statistics

Statistics about the backend can be retrieved by doing a GET request to:

	/statistics

This returns a JSON body like this:

	{
		"resources": [
			{
				"name": "user"
				"type": "collection"
				"count": 123,
				"size_mb": 0.117,
				"average_size_b": 599
			},
			{
				"name": "device"
				"type": "collection"
				"count": 56483,
				"size_mb": 12,
				"average_size_b": 558
			}
		]
	}

If you are only interested in certain resources, you can filter using the resource parameter like this:

	/statistics?resource=user,device

# Version

The Version of the software running can be obtain from a dedicated endpoint. The version can be set
at compile time with the following parameter:
-ldflags '-X github.com/relabs-tech/kurbisio/core/backend.Version="1.2.3"'

	/version

	This returns a Json body like this:

	{
		"version": "1.2.3"
	}
*/
package backend
