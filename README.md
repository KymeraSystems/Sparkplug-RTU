# Sparkplug

Sparkplug is a specification for MQTT enabled devices and applications to send and receive messages in a stateful way.  While MQTT is stateful my nature it doesn't ensure that all data on a receiving MQTT application is current or valid.  Sparkplug provides a mechanism for ensuring that remote device or application data is current and valid.

The examples here provide reference implementations in various languages and for various devices to show how the device/remote application must connect and disconnect from the MQTT server.  This includes device lifecycle messages such as the required birth and last will & testament messages that must be sent to ensure the device lifecycle state and data integrity.

The Sparkplug specification which explains these examples can be found here (coming soon).
