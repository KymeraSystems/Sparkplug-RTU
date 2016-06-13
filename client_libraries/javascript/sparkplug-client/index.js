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

var mqtt = require('mqtt'),
    kurapayload = require('./lib/kurapayload.js'),
    events = require('events'),
    util = require("util");

var getRequiredProperty = function(config, propName) {
    if (config[propName] !== undefined) {
        return config[propName];
    }
    throw new Error("Missing required configuration property '" + propName + "'");
};

/*
 * Sparkplug Client
 */
function SparkplugClient(config) {
    var serverUrl = getRequiredProperty(config, "serverUrl"),
        username = getRequiredProperty(config, "username"),
        password = getRequiredProperty(config, "password"),
        groupId = getRequiredProperty(config, "groupId"),
        edgeNode = getRequiredProperty(config, "edgeNode"),
        clientId = getRequiredProperty(config, "clientId"),
        bdSeq = 0,
        seq = 0,
        devices = [],
        client,

    // Increments a sequence number
    incrementSeqNum = function() {
        if (seq == 256) {
            return seq = 0;
        }
        return seq++;
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
            "metric" : [
                { "name" : "bdSeq", "value" : bdSeq, "type" : "int" },
                { "name" : "seq", "value" : incrementSeqNum(), "type" : "int" },
                { "name" : "Node Control/Rebirth", "value" : false, "type" : "boolean" }
            ]
        };
    },

    // Publishes BIRTH certificates for the edge node
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
    },

    // Logs a message alert to the console
    messageAlert = function(alert, topic, payload) {
        console.log("Message " + alert);
        console.log(" topic: " + topic);
        console.log(" payload: " + JSON.stringify(payload));
    };

    events.EventEmitter.call(this);

    // Configures and connects the client
    (function(sparkplugClient) {
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
            };

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
            // Emit the "rebirth" event to notify devices to send a birth
            console.log("Emmitting 'Rebirth' event");
            sparkplugClient.emit("rebirth");
        });

        client.on('message', function (topic, message) {
            var payload = kurapayload.parseKuraPayload(message),
                timestamp = payload.timestamp,
                metric = payload.metric,
                splitTopic;

            messageAlert("arrived", topic, payload);

            // Split the topic up into tokens
            splitTopic = topic.split("/");
            if (splitTopic[0] === "spAv1.0"
                    && splitTopic[1] === groupId
                    && splitTopic[2] === "NCMD"
                    && splitTopic[3] === edgeNode) {
                // Loop over the metrics looking for commands
                if (metric !== undefined && metric !== null) {
                    for (var i = 0; i < metric.length; i++) {
                        if (metric[i].name == "Node Control/Rebirth" && metric[i].value) {
                            console.log("Received 'Rebirth' command");
                            // Publish BIRTH certificate for the edge node
                            publishBirth(client);
                            // Emit the "rebirth" event
                            console.log("Emmitting 'Rebirth' event");
                            sparkplugClient.emit("rebirth");
                        }
                    }
                }
            } else if (splitTopic[0] === "spAv1.0"
                    && splitTopic[1] === groupId
                    && splitTopic[2] === "DCMD"
                    && splitTopic[3] === edgeNode) {
                console.log("Command recevied for device " + splitTopic[4]);
                // Emit the "command" event for the given deviceId
                sparkplugClient.emit("command", splitTopic[4], payload);
            }
        });
    }(this));

    this.publishDeviceData = function(deviceId, payload) {
        // Add seq number
        payload.metric.push({ "name" : "seq", "value" : incrementSeqNum(), "type" : "int" });
        // Publish
        console.log("Publishing DDATA for device " + deviceId);
        topic = "spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId;
        client.publish(topic, kurapayload.generateKuraPayload(payload));
        messageAlert("published", topic, payload);
    };

    this.publishDeviceBirth = function(deviceId, payload) {
        // Add seq number
        payload.metric.push({ "name" : "seq", "value" : incrementSeqNum(), "type" : "int" });
        // Publish
        console.log("Publishing DBIRTH for device " + deviceId);
        topic = "spAv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId;
        client.publish(topic, kurapayload.generateKuraPayload(payload));
        messageAlert("published", topic, payload);
    };

    this.publishDeviceDeath = function(deviceId, payload) {
        // Add seq number
        payload.metric = payload.metric !== undefined
                ? payload.metric
                : [];
        console.log("payload.metric: " + payload.metric);
        payload.metric.push({ "name" : "seq", "value" : incrementSeqNum(), "type" : "int" });
        // Publish
        console.log("Publishing DDEATH for device " + deviceId);
        topic = "spAv1.0/" + groupId + "/DDEATH/" + edgeNode + "/" + deviceId;
        client.publish(topic, kurapayload.generateKuraPayload(payload));
        messageAlert("published", topic, payload);
    };

    this.stop = function() {
        client.end();
    };
};

util.inherits(SparkplugClient, events.EventEmitter);

exports.newClient = function(config) {
    return new SparkplugClient(config);
};
