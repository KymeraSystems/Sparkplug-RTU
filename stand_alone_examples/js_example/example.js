/**
 * Copyright (c) 2012, 2016 Cirrus Link Solutions
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  which accompanies this distribution, and is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Cirrus Link Solutions
 */

var mqtt = require('mqtt'),
    kurapayload = require('./kurapayload.js');

/*
 * Main sample function which includes the run() function for running the sample
 */
var sample = (function () {
    var hwVersion = "Emulated Hardware",
        swVersion = "v1.0.0",
        // Configuration
        serverUrl = "tcp://localhost:1883",
        username = "admin",
        password = "changeme",
        groupId = "Sparkplug Devices",
        edgeNode = "JavaScript Edge Node",
        deviceId = "Emulated Device",
        clientId = "JavaScriptSimpleEdgeNode",
        publishPeriod = 5000,
        bdSeq = 0,
        seq = 0,    
    
    // Increments a sequence number    
    incrementSeqNum = function() {
        if (seq == 256) {
            return seq = 0;
        }
        return seq++;
    },
    
    // Generates a random integer
    randomInt = function() {
        return 1 + Math.floor(Math.random() * 10);
    }

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
    
    // Get DEATH payload
    getDeathPayload = function() {
        return {
            "timestamp" : new Date().getTime(),
            "metric" : [
                { "name" : "bdSeq", "value" : bdSeq, "type" : "int" },
            ]
        };
    },
    
    // Get BIRTH payload for the edge node
    getEdgeBirthPayload = function() {
        return {
            "timestamp" : new Date().getTime(),
            "position" : getPosition(),
            "metric" : [
                { "name" : "bdSeq", "value" : bdSeq, "type" : "int" },
                { "name" : "seq", "value" : incrementSeqNum(), "type" : "int" },
                { "name" : "Node Control/Rebirth", "value" : false, "type" : "boolean" }
            ]
        };
    },

    // Get BIRTH payload for the device
    getDeviceBirthPayload = function() {
        return {
            "timestamp" : new Date().getTime(),
            "metric" : [
                { "name" : "seq", "value" : incrementSeqNum(), "type" : "int" },
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
                { "name" : "seq", "value" : incrementSeqNum(), "type" : "int" },
                { "name" : "my_boolean", "value" : Math.random() > 0.5, "type" : "boolean" },
                { "name" : "my_double", "value" : Math.random() * 0.123456789, "type" : "double" },
                { "name" : "my_float", "value" : Math.random() * 0.123, "type" : "float" },
                { "name" : "my_int", "value" : randomInt(), "type" : "int" },
                { "name" : "my_long", "value" : randomInt() * 214748364700, "type" : "long" }
            ]
        };
    },

    // Publishes BIRTH certificates for the edge node and device
    publishBirth = function(client) {
        var payload, topic;
        // Reset sequence number
        seq = 0;
        
        // Publish BIRTH certificate for edge node
        console.log("Publishing Edge Node Birth");
        payload = getEdgeBirthPayload();
        topic = "spAv1.0/" + groupId + "/NBIRTH/" + edgeNode;
        client.publish(topic, kurapayload.generateKuraPayload(payload));
        messageAlert("published", topic, payload);

        // Publish BIRTH certificate for device
        console.log("Publishing device Birth");
        payload = getDeviceBirthPayload();
        topic = "spAv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId;
        client.publish(topic, kurapayload.generateKuraPayload(payload));
        messageAlert("published", topic, payload);
    },

    // Logs a message alert to the console
    messageAlert = function(alert, topic, payload) {
        console.log("Message " + alert);
        console.log(" topic: " + topic);
        console.log(" payload: " + JSON.stringify(payload));
    },
    
    // Runs the sample
    run = function() {
        var deathPayload = getDeathPayload(),
            // Client connection options
            clientOptions = {
                "clientId" : clientId,
                "clean" : true,
                "keepalive" : 30,
                "connectionTimeout" : 30,
                "username" : username,
                "password" : password,
                "will" : {
                    "topic" : "spAv1.0/" + groupId + "/NDEATH/" + edgeNode,
                    "payload" : kurapayload.generateKuraPayload(deathPayload),
                    "qos" : 0,
                    "retain" : false
                }
            },
            // Connect to the MQTT server
            client = mqtt.connect(serverUrl, clientOptions);
        
        client.on('connect', function () {
            console.log("Client has connected");
            
            // Subscribe to control/command messages for both the edge node and the attached devices
            console.log("Subscribing to control/command messages for both the edge node and the attached devices");
            client.subscribe("spAv1.0/" + groupId + "/NCMD/" + edgeNode + "/#", { "qos" : 0 });
            client.subscribe("spAv1.0/" + groupId + "/DCMD/" + edgeNode + "/#", { "qos" : 0 });
            
            // Publish BIRTH certificates
            publishBirth(client);
        });
        
        client.on('message', function (topic, message) {
            var payload = kurapayload.parseKuraPayload(message),
                timestamp = payload.timestamp,
                metric = payload.metric,
                inboundMetricMap = {},
                outboundMetric = [],
                outboundPayload, outboundTopic, splitTopic;
            
            messageAlert("arrived", topic, payload);
            
            // Loop over the metrics and store them in a map
            if (metric !== undefined && metric !== null) {
                for (var i = 0; i < metric.length; i++) {
                    var m = metric[i];
                    inboundMetricMap[m.name] = m.value;
                }
            }

            // Split the topic up into tokens
            splitTopic = topic.split("/");
            if (splitTopic[0] === "spAv1.0" 
                    && splitTopic[1] === groupId 
                    && splitTopic[2] === "NCMD" 
                    && splitTopic[3] === edgeNode) {
                console.log("Recieved node command: " + JSON.stringify(inboundMetricMap));
                if (inboundMetricMap["Node Control/Rebirth"] !== undefined 
                        && inboundMetricMap["Node Control/Rebirth"] !== null
                        && inboundMetricMap["Node Control/Rebirth"]) {
                    console.log("Received 'Rebirth' command");
                    // Publish BIRTH certificates
                    publishBirth(client);
                }
            } else if (splitTopic[0] === "spAv1.0" 
                    && splitTopic[1] === groupId 
                    && splitTopic[2] === "DCMD" 
                    && splitTopic[3] === edgeNode) {
                console.log("Command recevied for device " + splitTopic[4]);
                outboundMetric.push({ "name" : "seq", "value" : incrementSeqNum(), "type" : "int" });
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
                
                var outboundPayload = {
                    "timestamp" : new Date().getTime(),
                    "position" : getPosition(),
                    "metric" : outboundMetric
                };

                console.log("Publishing device data");
                outboundTopic = "spAv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId;
                client.publish(outboundTopic, kurapayload.generateKuraPayload(outboundPayload));
                messageAlert("published", outboundTopic, outboundPayload);
            }   
        });
        
        for (var i = 1; i < 101; i++) {
            // Set up a publish for i*publishPeriod milliseconds from now
            setTimeout(function() {
                console.log("Publishing device data");
                outboundPayload = getDataPayload();
                outboundTopic = "spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId;
                client.publish(outboundTopic, kurapayload.generateKuraPayload(outboundPayload));
                messageAlert("published", outboundTopic, outboundPayload);
                
                // End the client connection after the last publish
                if (i === 100) {
                    client.end();
                }
            }, i*publishPeriod);
        }
    };
    
    return {run:run};
}());

// Run the sample
sample.run();
