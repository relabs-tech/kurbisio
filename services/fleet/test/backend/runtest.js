
//node.js deps

//npm deps

//app deps

var http = require('http'),
   https = require('https'),
   minimist = require('minimist'),
   fs = require('fs');

function genericRequest(method, path, object, next) {
   const data = JSON.stringify(object)

   var opts = {
      host: 'localhost',
      port: 3000,
      path: path,
      method: method,
      body: data,

      headers: {
         'Content-Type': 'application/json',
         'Content-Length': data.length
      }
   }

   let req = http.request(opts, function (res) {
      let data = '';

      // A chunk of data has been recieved.
      res.on('data', (chunk) => {
         data += chunk;
      });
      // The whole response has been received. 
      res.on('end', () => {
         console.log("status code:", res.statusCode)
         if (res.statusCode == 204) {
            next({})
         } else {
            let response = JSON.parse(data)
            next(response)
         }
      });

   })

   req.on("error", (err) => {
      console.log("Error: " + err.message);
   })
   req.write(data)
   req.end();

}

function genericPost(path, object, next) {
   genericRequest('POST', path, object, next)
}

function genericPut(path, object, next) {
   genericRequest('PUT', path, object, next)
}

function emptyPut(path, next) {

   var opts = {
      host: 'localhost',
      port: 3000,
      path: path,
      method: 'PUT',
      headers: {
         'Content-Type': 'application/json',
         'Content-Length': 0
      }
   }

   let req = http.request(opts, function (res) {
      let data = '';

      // A chunk of data has been recieved.
      res.on('data', (chunk) => {
         data += chunk;
      });
      // The whole response has been received. 
      res.on('end', () => {
         console.log("status code:", res.statusCode)
         next()
      });
   })

   req.on("error", (err) => {
      console.log("Error: " + err.message);
   })
   req.end();

}


var fleet_id
function createFleet(next) {
   console.log("create fleet")
   genericPost("/fleets",
      {
         properties: {
            name: "Number One",
            description: "The number one fleet in this world"
         }
      }, function (response) {
         console.log(response)
         fleet_id = response.fleet_id
         console.log("fleet_id:", fleet_id)
         next()
      })
}

var device_id
function createDevice(next) {
   console.log("create device")
   genericPost("/devices",
      {
         thing: "Device1",
         provisioning_status: "waiting",
         properties: {
            name: "The First Device",
         }
      }, function (response) {
         console.log(response)
         device_id = response.device_id
         console.log("device_id:", device_id)
         next()
      })
}

var user_id
function createUser(next) {
   console.log("create user")
   genericPost("/fleets/" + fleet_id + "/users",
      {
         properties: {
            first_name: "Matthias",
            last_name: "Ettrich"
         }
      }, function (response) {
         console.log(response)
         user_id = response.user_id
         console.log("user_id:", user_id)
         next()
      })
}

function assignDeviceToFleet(next) {
   console.log("assign device to fleet")
   emptyPut("/fleets/" + fleet_id + "/devices/" + device_id,
      function (response) {
         next()
      })
}

function requestDeviceConfiguration(next) {
   console.log("request device configuration through twin")
   genericPut("/devices/" + device_id + "/twin/configuration/request",
      {
         "version": "4.0"
      },
      function (response) {
         next()
      })
}


// and now for something completely different: callback hell
createFleet(
   () => createDevice(
      () => createUser(
         () => assignDeviceToFleet(
            () => requestDeviceConfiguration(
               () => { console.log("done!") }
            )
         )
      )
   )
)


