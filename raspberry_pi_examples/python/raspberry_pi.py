#!/usr/bin/python
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
import pibrella
import kurapayload_pb2
import time
import random

serverUrl = "192.168.1.1"
myGroupId = "Sparkplug Devices"
myNodeName = "Python Raspberry Pi"
mySubNodeName = "Pibrella"
myUsername = "admin"
myPassword = "changeme"
seq = 0
bdSeq = 0

######################################################################
# Button press event handler
######################################################################
def button_changed(pin):
    outboundPayload = kurapayload_pb2.KuraPayload()
    if pin.read() == 1:
	print("You pressed the button!")
    else:
	print("You released the button!")
    outboundPayload.timestamp = int(round(time.time() * 1000))
    addMetric(outboundPayload, "seq", "INT32", getSeqNum())
    addMetric(outboundPayload, "button", "BOOL", pin.read());
    byteArray = bytearray(outboundPayload.SerializeToString())
    client.publish("spv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + mySubNodeName, byteArray, 0, False)

######################################################################
# Input change event handler
######################################################################
def input_a_changed(pin):
    input_changed("input_a", pin)
def input_b_changed(pin):
    input_changed("input_b", pin)
def input_c_changed(pin):
    input_changed("input_c", pin)
def input_d_changed(pin):
    input_changed("input_d", pin)
def input_changed(name, pin):
    outboundPayload = kurapayload_pb2.KuraPayload()
    outboundPayload.timestamp = int(round(time.time() * 1000))
    addMetric(outboundPayload, "seq", "INT32", getSeqNum())
    addMetric(outboundPayload, name, "BOOL", pin.read());
    byteArray = bytearray(outboundPayload.SerializeToString())
    client.publish("spv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + mySubNodeName, byteArray, 0, False)

######################################################################
# Helper method for getting the next sequence number
######################################################################
def getSeqNum():
    global seq
    retVal = seq
    seq += 1
    return retVal
######################################################################

######################################################################
# Helper method for getting the next birth/death sequence number
######################################################################
def getBdSeqNum():
    global bdSeq
    retVal = bdSeq
    bdSeq += 1
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
    client.subscribe("spv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#")
    client.subscribe("spv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#")
######################################################################

######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    #print(msg.topic+" "+str(msg.payload))
    print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if tokens[0] == "spv1.0" and tokens[1] == myGroupId and tokens[2] == "DCMD" and tokens[3] == myNodeName:
	inboundPayload = kurapayload_pb2.KuraPayload()
	inboundPayload.ParseFromString(msg.payload)
	for metric in inboundPayload.metric:
	    outboundPayload = kurapayload_pb2.KuraPayload()
	    outboundPayload.timestamp = int(round(time.time() * 1000))
	    addMetric(outboundPayload, "seq", "INT32", getSeqNum())
	    print "Tag Name: " + metric.name
	    if metric.name == "output_e":
		pibrella.output.e.write(metric.bool_value)
		addMetric(outboundPayload, "output_e", "BOOL", pibrella.output.e.read())
	    elif metric.name == "output_f":
		pibrella.output.f.write(metric.bool_value)
		addMetric(outboundPayload, "output_f", "BOOL", pibrella.output.f.read())
	    elif metric.name == "output_g":
		pibrella.output.g.write(metric.bool_value)
		addMetric(outboundPayload, "output_g", "BOOL", pibrella.output.g.read())
	    elif metric.name == "output_h":
		pibrella.output.h.write(metric.bool_value)
		addMetric(outboundPayload, "output_h", "BOOL", pibrella.output.h.read())
	    elif metric.name == "led_green":
		if metric.bool_value:
		    pibrella.light.green.on()
		else:
		    pibrella.light.green.off()
		addMetric(outboundPayload, "led_green", "BOOL", pibrella.light.green.read())
	    elif metric.name == "led_red":
		if metric.bool_value:
		    pibrella.light.red.on()
		else:
		    pibrella.light.red.off()
		addMetric(outboundPayload, "led_red", "BOOL", pibrella.light.red.read())
	    elif metric.name == "led_yellow":
		if metric.bool_value:
		    pibrella.light.yellow.on()
		else:
		    pibrella.light.yellow.off()
		addMetric(outboundPayload, "led_yellow", "BOOL", pibrella.light.yellow.read())
	    elif metric.name == "buzzer_fail":
		pibrella.buzzer.fail()
	    elif metric.name == "buzzer_success":
		pibrella.buzzer.success()

	    byteArray = bytearray(outboundPayload.SerializeToString())
	    client.publish("spv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + mySubNodeName, byteArray, 0, False)

    print "done publishing"
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
client.will_set("spv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, 0, False)
client.connect(serverUrl, 1883, 60)

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
payload.timestamp = int(round(time.time() * 1000))
addMetric(payload, "bdSeq", "INT32", bdSeq)
addMetric(payload, "seq", "INT32", getSeqNum())

# Publish the node birth certificate
byteArray = bytearray(payload.SerializeToString())
client.publish("spv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, byteArray, 0, False)

# Set up the button press event handler
pibrella.button.changed(button_changed)
pibrella.input.a.changed(input_a_changed)
pibrella.input.b.changed(input_b_changed)
pibrella.input.c.changed(input_c_changed)
pibrella.input.d.changed(input_d_changed)

# Set up the input metrics
pvPayload = kurapayload_pb2.KuraPayload()
addMetric(pvPayload, "input_a", "BOOL", pibrella.input.a.read())
addMetric(pvPayload, "input_b", "BOOL", pibrella.input.b.read())
addMetric(pvPayload, "input_c", "BOOL", pibrella.input.c.read())
addMetric(pvPayload, "input_d", "BOOL", pibrella.input.d.read())

# Set up the output states on first run so Ignition and MQTT Engine are aware of them
addMetric(pvPayload, "output_e", "BOOL", pibrella.output.e.read())
addMetric(pvPayload, "output_f", "BOOL", pibrella.output.f.read())
addMetric(pvPayload, "output_g", "BOOL", pibrella.output.g.read())
addMetric(pvPayload, "output_h", "BOOL", pibrella.output.h.read())
addMetric(pvPayload, "led_green", "BOOL", pibrella.light.green.read())
addMetric(pvPayload, "led_red", "BOOL", pibrella.light.red.read())
addMetric(pvPayload, "led_yellow", "BOOL", pibrella.light.yellow.read())
addMetric(pvPayload, "button", "BOOL", pibrella.button.read())
addMetric(pvPayload, "buzzer_fail", "BOOL", 0)
addMetric(pvPayload, "buzzer_success", "BOOL", 0)

# Set up the propertites payload
parameterPayload = kurapayload_pb2.KuraPayload()
addMetric(parameterPayload, "device_hw_version", "STRING", "PFC_1.1")
addMetric(parameterPayload, "firmware_version", "STRING", "1.4.2")

# Publish the initial data with the Device BIRTH certificate
pvMapByteArray = pvPayload.SerializeToString()
parameterByteArray = parameterPayload.SerializeToString()
totalPayload = kurapayload_pb2.KuraPayload()
totalPayload.timestamp = int(round(time.time() * 1000))
addMetric(totalPayload, "seq", "INT32", getSeqNum())
addMetric(totalPayload, "pv_map", "BYTES", pvMapByteArray)
addMetric(totalPayload, "device_parameters", "BYTES", parameterByteArray)
totalByteArray = bytearray(totalPayload.SerializeToString())
client.publish("spv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + mySubNodeName, totalByteArray, 0, False)

# Sit and wait for inbound or outbound events
while True:
    time.sleep(.1)
    client.loop()
