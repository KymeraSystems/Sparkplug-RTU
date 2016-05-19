#!/usr/local/bin/python
######################################################################
# Copyright (c) 2012, 2016 Cirrus Link Solutions
#
#  All rights reserved. This program and the accompanying materials
#  are made available under the terms of the Eclipse Public License v1.0
#  which accompanies this distribution, and is available at
#  http://www.eclipse.org/legal/epl-v10.html
#
# Contributors:
#   Cirrus Link Solutions
######################################################################

import paho.mqtt.client as mqtt
import kurapayload_pb2
import time
import random

serverUrl = "localhost"
myGroupId = "Sparkplug Devices"
myNodeName = "Python Edge Node"
mySubNodeName = "Emulated Device"
publishPeriod = 5000
myUsername = "admin"
myPassword = "changeme"
seqNum = 0
bdSeq = 0

######################################################################
# Helper method for getting the next sequence number
######################################################################
def getSeqNum():
    global seqNum
    retVal = seqNum
    seqNum += 1
    if seqNum == 256:
	seqNum = 0
    return retVal
######################################################################

######################################################################
# Helper method for getting the next birth/death sequence number
######################################################################
def getBdSeqNum():
    global bdSeq
    retVal = bdSeq
    bdSeq += 1
    if bdSeq == 256:
        bdSeq = 0
    return retVal
######################################################################

######################################################################
# Helper method for adding metrics to a payload
######################################################################
def addMetric(payload, name, type, value):
    if type == "DOUBLE":
	metric = payload.metric.add()
	metric.name = name
	metric.type = kurapayload_pb2.KuraPayload.KuraMetric.DOUBLE
	metric.double_value = value
    elif type == "FLOAT":
	metric = payload.metric.add()
	metric.name = name
	metric.type = kurapayload_pb2.KuraPayload.KuraMetric.FLOAT
	metric.float_value = value
    elif type == "INT64":
	metric = payload.metric.add()
	metric.name = name
	metric.type = kurapayload_pb2.KuraPayload.KuraMetric.INT64
	metric.long_value = value
    elif type == "INT32":
	metric = payload.metric.add()
	metric.name = name
	metric.type = kurapayload_pb2.KuraPayload.KuraMetric.INT32
	metric.int_value = value
    elif type == "BOOL":
	metric = payload.metric.add()
	metric.name = name
	metric.type = kurapayload_pb2.KuraPayload.KuraMetric.BOOL
	metric.bool_value = value
    elif type == "STRING":
	metric = payload.metric.add()
	metric.name = name
	metric.type = kurapayload_pb2.KuraPayload.KuraMetric.STRING
	metric.string_value = value
    elif type == "BYTES":
	metric = payload.metric.add()
	metric.name = name
	metric.type = kurapayload_pb2.KuraPayload.KuraMetric.BYTES
	metric.bytes_value = value

    return payload
######################################################################

######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    global myGroupId
    global myNodeName
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("spAv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#")
    client.subscribe("spAv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#")
######################################################################

######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if tokens[0] == "spAv1.0" and tokens[1] == myGroupId and tokens[2] == "DCMD" and tokens[3] == myNodeName:
	inboundPayload = kurapayload_pb2.KuraPayload()
	inboundPayload.ParseFromString(msg.payload)
	outboundPayload = kurapayload_pb2.KuraPayload()
        outboundPayload.timestamp = int(round(time.time() * 1000))
        addMetric(outboundPayload, "seq", "INT32", getSeqNum())
	for metric in inboundPayload.metric:
	    if metric.name == "Outputs/0":
		print "Outputs/0: " + str(metric.bool_value)
		addMetric(outboundPayload, "Inputs/0", "BOOL", metric.bool_value)
		addMetric(outboundPayload, "Outputs/0", "BOOL", metric.bool_value)
	    elif metric.name == "Outputs/1":
		print "Outputs/1: " + str(metric.int_value)
		addMetric(outboundPayload, "Inputs/1", "INT32", metric.int_value)
		addMetric(outboundPayload, "Outputs/1", "INT32", metric.int_value)
	    elif metric.name == "Outputs/2":
		print "Outputs/2: " + str(metric.float_value)
		addMetric(outboundPayload, "Inputs/2", "FLOAT", metric.float_value)
		addMetric(outboundPayload, "Outputs/2", "FLOAT", metric.float_value)

	byteArray = bytearray(outboundPayload.SerializeToString())
	client.publish("spAv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + mySubNodeName, byteArray, 0, False)
    elif tokens[0] == "spAv1.0" and tokens[1] == myGroupId and tokens[2] == "NCMD" and tokens[3] == myNodeName:
        inboundPayload = kurapayload_pb2.KuraPayload()
        inboundPayload.ParseFromString(msg.payload)
        for metric in inboundPayload.metric:
            if metric.name == "Rebirth":
                publishBirth()

    print "done publishing"

######################################################################

######################################################################
# Publish the Birth certificate
######################################################################
def publishBirth():
    # Create the node birth payload with a position
    payload = kurapayload_pb2.KuraPayload()
    position = payload.position
    position.altitude = 319
    position.heading = 0
    position.latitude = 38.83667239
    position.longitude = -94.67176706
    position.precision = 2.0
    position.satellites = 8
    position.speed = 0
    position.status = 3
    position.timestamp = int(round(time.time() * 1000))

    # Add a timestamp and sequence numbers to the payload
    seqNum = 0
    payload.timestamp = int(round(time.time() * 1000))
    addMetric(payload, "bdSeq", "INT32", bdSeq)
    addMetric(payload, "seq", "INT32", getSeqNum())

    # Publish the node birth certificate
    byteArray = bytearray(payload.SerializeToString())
    client.publish("spAv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, byteArray, 0, False)

    # Setup the inputs
    payload = kurapayload_pb2.KuraPayload()
    payload.timestamp = int(round(time.time() * 1000))
    addMetric(payload, "seq", "INT32", getSeqNum())

    addMetric(payload, "my_boolean", "BOOL", random.choice([True, False]))
    addMetric(payload, "my_float", "FLOAT", random.random())
    addMetric(payload, "my_int", "INT32", random.randint(0,100))
    addMetric(payload, "my_long", "INT64", random.getrandbits(60))
    addMetric(payload, "Inputs/0", "BOOL", True)
    addMetric(payload, "Inputs/1", "INT32", 0)
    addMetric(payload, "Inputs/2", "FLOAT", 1.23)

    # Set up the output states on first run so Ignition and MQTT Engine are aware of them
    addMetric(payload, "Outputs/0", "BOOL", True)
    addMetric(payload, "Outputs/1", "INT32", 0)
    addMetric(payload, "Outputs/2", "FLOAT", 1.23)

    # Set up the propertites
    addMetric(payload, "Properties/Hardware Version", "STRING", "PFC_1.1")
    addMetric(payload, "Properties/Firmware Version", "STRING", "1.4.2")

    # Publish the initial data with the Device BIRTH certificate
    totalByteArray = bytearray(payload.SerializeToString())
    client.publish("spAv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + mySubNodeName, totalByteArray, 0, False)

######################################################################

# Create the DEATH payload
deathPayload = kurapayload_pb2.KuraPayload()
deathPayload.timestamp = int(round(time.time() * 1000))
addMetric(deathPayload, "bdSeq", "INT32", getBdSeqNum())
deathByteArray = bytearray(deathPayload.SerializeToString())

# Start of main program - Set up the MQTT client connection
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
client.will_set("spAv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, 0, False)
client.connect(serverUrl, 1883, 60)

publishBirth()

while True:
    payload = kurapayload_pb2.KuraPayload()
    payload.timestamp = int(round(time.time() * 1000))
    addMetric(payload, "seq", "INT32", getSeqNum())

    addMetric(payload, "my_boolean", "BOOL", random.choice([True, False]))
    addMetric(payload, "my_float", "FLOAT", random.random())
    addMetric(payload, "my_int", "INT32", random.randint(0,100))
    addMetric(payload, "my_long", "INT64", random.getrandbits(60))

    # Publish the data
    byteArray = bytearray(payload.SerializeToString())
    client.publish("spAv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + mySubNodeName, byteArray, 0, False)

    for _ in range(50):
	time.sleep(.1)
	client.loop()
