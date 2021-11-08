// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

/*Package iot provides core IoT functionality

It contains two RESTful apis which implements device authorization and a device twin,
and a MQTT broker which also implements device authorization and a device twin.

The RESTful api itself can be used with different MQTT brokers, such as AWS IoT. It only
needs a mesage publisher interface to publish device twin message to the device. The broker
does satisfy this interface, hence broker and api work together well.

TODO

In order to fully support AWS IoT, the API must be made slightly more configurable for
device authorization, so that the certificates can also be added to AWS IoT itself.

*/
package iot
