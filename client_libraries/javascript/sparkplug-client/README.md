Sparkplug Client
=========

A client library providing a MQTT client for MQTT device communication using the
Sparkplug Specification from Cirrus Link Solutions.

## Installation

  npm install sparkplug-client

## Usage

var sparkplug = require('sparkplug-client'),
    config = {
        'serverUrl' : 'tcp://localhost:1883',
        'username' : 'username',
        'password' : 'password',
        'groupId' : 'Sparkplug Devices',
        'edgeNode' : 'Test Edge Node',
        'clientId' : 'JavaScriptSimpleEdgeNode'
    },
    client = sparkplug.newClient(config);

client.on('rebirth', function () {
    console.log("received 'rebirth' event");
});


client.on('command', function (device, payload) {
    console.log("received 'command' event");
    console.log("device: " + device);
    console.log("payload: " + payload);
});

## Release History

* 1.0.0 Initial release
