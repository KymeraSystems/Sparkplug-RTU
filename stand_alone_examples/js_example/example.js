/**
 * Copyright (c) 2016 Cirrus Link Solutions
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Cirrus Link Solutions
 */

var SparkplugClient = require('./lib/sparkplug-client');

/*
 * Main sample function which includes the run() function for running the sample
 */
var sample = (function () {
    var config = {
            'serverUrl' : 'tcp://localhost:1883',
            'username' : 'admin',
            'password' : 'changeme',
            'groupId' : 'Sparkplug Devices',
            'edgeNode' : 'JavaScript Edge Node',
            'clientId' : 'JavaScriptSimpleEdgeNode'
        },
        hwVersion = 'Emulated Hardware',
        swVersion = 'v1.0.0',
        deviceId = 'Emulated Device',
        sparkPlugClient,
        publishPeriod = 5000,
        
    // Generates a random integer
    randomInt = function() {
        return 1 + Math.floor(Math.random() * 10);
    },

    // Gets a new Kura position
    getPosition = function() {
        return {
            "latitude" : 38.83667239,
            "longitude" : -94.67176706,
            "altitude" : 319,
            "precision" : 2.0,
            "heading" : 0,
            "speed" : 0,
            "timestamp" : new Date().getTime(),
            "satellites" : 8,
            "status" : 3
        };
    },

    // Get BIRTH payload for the device
    getDeviceBirthPayload = function() {
        return {
            "timestamp" : new Date().getTime(),
            "metric" : [
                { "name" : "my_boolean", "value" : Math.random() > 0.5, "type" : "boolean" },
                { "name" : "my_double", "value" : Math.random() * 0.123456789, "type" : "double" },
                { "name" : "my_float", "value" : Math.random() * 0.123, "type" : "float" },
                { "name" : "my_int", "value" : randomInt(), "type" : "int" },
                { "name" : "my_long", "value" : randomInt() * 214748364700, "type" : "long" },
                { "name" : "Inputs/0", "value" :  true, "type" : "boolean" },
                { "name" : "Inputs/1", "value" :  0, "type" : "int" },
                { "name" : "Inputs/2", "value" :  1.23, "type" : "float" },
                { "name" : "Outputs/0", "value" :  true, "type" : "boolean" },
                { "name" : "Outputs/1", "value" :  0, "type" : "int" },
                { "name" : "Outputs/2", "value" :  1.23, "type" : "float" },
                { "name" : "Properties/hw_version", "value" :  hwVersion, "type" : "string" },
                { "name" : "Properties/sw_version", "value" :  swVersion, "type" : "string" }
            ]
        };
    },
    
    // Get data payload for the device
    getDataPayload = function() {
        return {
            "timestamp" : new Date().getTime(),
            "position" : getPosition(),
            "metric" : [
                { "name" : "my_boolean", "value" : Math.random() > 0.5, "type" : "boolean" },
                { "name" : "my_double", "value" : Math.random() * 0.123456789, "type" : "double" },
                { "name" : "my_float", "value" : Math.random() * 0.123, "type" : "float" },
                { "name" : "my_int", "value" : randomInt(), "type" : "int" },
                { "name" : "my_long", "value" : randomInt() * 214748364700, "type" : "long" }
            ]
        };
    },
    
    // Runs the sample
    run = function() {
        // Create the SparkplugClient
        console.log("config: " + JSON.stringify(config));
        sparkplugClient = SparkplugClient.newClient(config);
        
        // Create 'rebirth' handler
        sparkplugClient.on('rebirth', function () {
            // Publish BIRTH certificate
            sparkplugClient.publishDeviceBirth(deviceId, getDeviceBirthPayload());
        });
        
        // Create 'command' handler
        sparkplugClient.on('command', function (deviceId, payload) {
            var timestamp = payload.timestamp,
                metric = payload.metric,
                inboundMetricMap = {},
                outboundMetric = [],
                outboundPayload;
            
            console.log("Command recevied for device " + deviceId);
            
            // Loop over the metrics and store them in a map
            if (metric !== undefined && metric !== null) {
                for (var i = 0; i < metric.length; i++) {
                    var m = metric[i];
                    inboundMetricMap[m.name] = m.value;
                }
            }

            if (inboundMetricMap["Outputs/0"] !== undefined && inboundMetricMap["Outputs/0"] !== null) {
                console.log("Outputs/0: " + inboundMetricMap["Outputs/0"]);
                outboundMetric.push({ "name" : "Inputs/0", "value" : inboundMetricMap["Outputs/0"], "type" : "boolean" });
                outboundMetric.push({ "name" : "Outputs/0", "value" : inboundMetricMap["Outputs/0"], "type" : "boolean" });
                console.log("Updated value for Inputs/0 " + inboundMetricMap["Outputs/0"]);
            } else if (inboundMetricMap["Outputs/1"] !== undefined && inboundMetricMap["Outputs/1"] !== null) {
                console.log("Outputs/1: " + inboundMetricMap["Outputs/1"]);
                outboundMetric.push({ "name" : "Inputs/1", "value" : inboundMetricMap["Outputs/1"], "type" : "int" });
                outboundMetric.push({ "name" : "Outputs/1", "value" : inboundMetricMap["Outputs/1"], "type" : "int" });
                console.log("Updated value for Inputs/1 " + inboundMetricMap["Outputs/1"]);
            } else if (inboundMetricMap["Outputs/2"] !== undefined && inboundMetricMap["Outputs/2"] !== null) {
                console.log("Outputs/2: " + inboundMetricMap["Outputs/2"]);
                outboundMetric.push({ "name" : "Inputs/2", "value" : inboundMetricMap["Outputs/2"], "type" : "float" });
                outboundMetric.push({ "name" : "Outputs/2", "value" : inboundMetricMap["Outputs/2"], "type" : "float" });
                console.log("Updated value for Inputs/2 " + inboundMetricMap["Outputs/2"]);
            }

            outboundPayload = {
                    "timestamp" : new Date().getTime(),
                    "position" : getPosition(),
                    "metric" : outboundMetric
            };

            // Publish device data
            sparkplugClient.publishDeviceData(deviceId, outboundPayload);             
        });
        
        for (var i = 1; i < 101; i++) {
            // Set up a device data publish for i*publishPeriod milliseconds from now
            setTimeout(function() {
                // Publish device data
                sparkplugClient.publishDeviceData(deviceId, getDataPayload());
                
                // End the client connection after the last publish
                if (i === 100) {
                    sparkplugClient.stop();
                }
            }, i*publishPeriod);
        }
    };
    
    return {run:run};
}());

// Run the sample
sample.run();