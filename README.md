# Sparkplug-RTU

Sparkplug is a specification for MQTT enabled devices and applications to send and receive messages in a stateful way.  While MQTT is stateful my nature it doesn't ensure that all data on a receiving MQTT application is current or valid.  Sparkplug provides a mechanism for ensuring that remote device or application data is current and valid.

The examples contained here create a gas meter simulation device, for testing an MQTT simulation.  Upon first run, the jar will create a client connecting to a broker on your localhost, if present.  It will create a file, rtu-conifig.json, which will allow you to override various default settings.

THe data also maps to a modbus server as well, allowing for data usage comparison between MQTT and Modbus.

The Sparkplug specification which explains these examples can be found here: https://s3.amazonaws.com/cirrus-link-com/Sparkplug+Specification+Version+1.0.pdf

Tutorials showing how to use this reference code can be found here:
https://docs.chariot.io/display/CLD/Tutorials
