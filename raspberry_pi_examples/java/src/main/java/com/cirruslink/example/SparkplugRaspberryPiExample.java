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
package com.cirruslink.example;

import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.kura.core.cloud.CloudPayloadEncoder;
import org.eclipse.kura.core.cloud.CloudPayloadProtoBufDecoderImpl;
import org.eclipse.kura.core.cloud.CloudPayloadProtoBufEncoderImpl;
import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.message.KuraPosition;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;

import com.pi4j.component.button.ButtonState;
import com.pi4j.component.button.ButtonStateChangeEvent;
import com.pi4j.component.button.ButtonStateChangeListener;
import com.pi4j.device.pibrella.Pibrella;
import com.pi4j.device.pibrella.PibrellaInput;
import com.pi4j.device.pibrella.PibrellaOutput;
import com.pi4j.device.pibrella.impl.PibrellaDevice;
import com.pi4j.io.gpio.PinState;
import com.pi4j.io.gpio.event.GpioPinDigitalStateChangeEvent;
import com.pi4j.io.gpio.event.GpioPinListenerDigital;

/*
 * This is a VERY simple implementation of an MQTT Edge of Network Node (EoN Node) and an associated 
 * Device that follows the Sparkplug specification. This is NOT intended to be production quality code
 * but rather a heavily commented tutorial covering all of the basic components defined in the Sparkplug
 * specification document.
 * 
 * The version of Raspberry Pi used in this example is the Raspberry Pi 2 Model B. The plug on I/O board
 * used for the real world I/O is a Pibrella I/O board, hardware version 3.0.
 * 
 */

public class SparkplugRaspberryPiExample implements MqttCallback {

	private static final Pibrella pibrella = new PibrellaDevice();

	// HW/SW versions
	private static final String HW_VERSION = "Raspberry Pi 2 model B";
	private static final String SW_VERSION = "v1.0.0";

	// Configuration
	private String serverUrl = "tcp://192.168.0.17:1883"; // Change to point to
															// your MQTT Server
	private String groupId = "Sparkplug Devices";
	private String edgeNode = "Java Raspberry Pi";
	private String deviceId = "Pibrella";
	private String clientId = "javaPibrellaClientId";
	private String username = "admin";
	private String password = "changeme";
	private ExecutorService executor;
	private MqttClient client;

	// Some control and parameter points for this demo
	private int configChangeCount = 1;
	private int scanRateMs = 1000;
	private long upTimeMs = 0;
	private long upTimeStart = System.currentTimeMillis();
	private int buttonCounter = 0;
	private int buttonCounterSetpoint = 10;

	private int bdSeq = 0;
	private int seq = 0;

	private Object lock = new Object();

	public static void main(String[] args) {
		SparkplugRaspberryPiExample example = new SparkplugRaspberryPiExample();
		example.run();
	}

	public void run() {
		try {
			// Create the Raspberry Pi Pibrella board listeners
			createPibrellaListeners();

			// Thread pool for outgoing published messages
			executor = Executors.newFixedThreadPool(1);

			// Wait for 'ctrl c' to exit
			while (true) {
				//
				// This is a very simple loop for the demo that tries to keep the MQTT Session 
				// up, and published the Up Time metric based on the current value of
				// the scanRateMs process variable.
				//
				if (client == null || !client.isConnected()) {
					establishMqttSession();
					publishBirth();
				} else {
					synchronized (lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						outboundPayload.addMetric("Up Time ms", System.currentTimeMillis() - upTimeStart);
						// Publish current Up Time
						executor.execute(new Publisher("spAv1.0/" + groupId + "/NDATA/" + edgeNode, outboundPayload));

					}
				}
				Thread.sleep(scanRateMs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Establish an MQTT Session with Sparkplug defined Death Certificate. It may not be
	 * Immediately intuitive that the Death Certificate is created prior to publishing the
	 * Birth Certificate, but the Death Certificate is actually part of the MQTT Session 
	 * establishment. For complete details of the actual MQTT wire protocol refer to the 
	 * latest OASyS MQTT V3.1.1 standards at:
	 * http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html
	 * 
	 * @return true = MQTT Session Established
	 */
	public boolean establishMqttSession() {
		try {

			//
			// Setup the MQTT connection parameters using the Paho MQTT Client.
			//
			MqttConnectOptions options = new MqttConnectOptions();
			// MQTT session parameters Clean Start = true
			options.setCleanSession(true);
			// Session connection attempt timeout period in seconds
			options.setConnectionTimeout(10);
			// MQTT session parameter Keep Alive Period in Seconds
			options.setKeepAliveInterval(30);
			// MQTT Client Username
			options.setUserName(username);
			// MQTT Client Password
			options.setPassword(password.toCharArray());
			//
			// Build up the Death Certificate MQTT Payload. Note that the Death
			// Certificate payload sequence number
			// is not tied to the normal message sequence numbers.
			//
			KuraPayload deathPayload = new KuraPayload();
			deathPayload.setTimestamp(new Date());
			deathPayload = addBdSeqNum(deathPayload);
			CloudPayloadEncoder deathEncoder = new CloudPayloadProtoBufEncoderImpl(deathPayload);
			//
			// Setup the Death Certificate Topic/Payload into the MQTT session
			// parameters
			//
			options.setWill("spAv1.0/" + groupId + "/NDEATH/" + edgeNode, deathEncoder.getBytes(), 0, false);

			//
			// Create a new Paho MQTT Client
			//
			client = new MqttClient(serverUrl, clientId);
			//
			// Using the parameters set above, try to connect to the define MQTT
			// server now.
			//
			System.out.println("Trying to establish an MQTT Session to the MQTT Server @ :" + serverUrl);
			client.connect(options);
			System.out.println("MQTT Session Established");
			client.setCallback(this);
			//
			// With a successful MQTT Session in place, now issue subscriptions
			// for the EoN Node and Device "Command" Topics of 'NCMD' and 'DCMD'
			// defined in Sparkplug
			//
			client.subscribe("spAv1.0/" + groupId + "/NCMD/" + edgeNode + "/#", 0);
			client.subscribe("spAv1.0/" + groupId + "/DCMD/" + edgeNode + "/#", 0);
		} catch (Exception e) {
			System.out.println("Error Establishing an MQTT Session:");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	
	/**
	 * Publish the EoN Node Birth Certificate and the Device Birth Certificate
	 * per the Sparkplug Specification
	 */
	public void publishBirth() {
		try {
			synchronized (lock) {

				//
				// Create the EoN Node Birth Certificate per the Sparkplug
				// specification
				//

				//
				// Create the EoN Node BIRTH payload with any number of
				// read/write properties for this node. These parameters will
				// appear in
				// folders under this Node in the Ignition tag structure.
				//
				KuraPayload payload = new KuraPayload();

				seq = 0; // Since this is a birth - reset the seq number
							// Note that message sequence numbers will appear in
							// the "Node Metrics" folder in Ignition.
				payload = addSeqNum(payload);
				payload.addMetric("bdSeq", bdSeq);
				payload.setTimestamp(new Date());

				payload.addMetric("Up Time ms", System.currentTimeMillis() - upTimeStart);

				payload.addMetric("Node Control/Reboot", false);
				payload.addMetric("Node Control/Rebirth", false);
				payload.addMetric("Node Control/Next Server", false);
				payload.addMetric("Node Control/Scan Rate ms", scanRateMs);

				payload.addMetric("Properties/Node Manf", "Element 14");
				payload.addMetric("Properties/Hardware Version", HW_VERSION);
				payload.addMetric("Properties/Software Version", SW_VERSION);
				payload.addMetric("Properties/Config Change Count", configChangeCount);
				payload.addMetric("Properties/Ip_adr", "192.168.0.55");

				// Build a GPS Position object for the payload. Note that the
				// Position object is optional.
				// The Position parameters will be populated in "Position"
				// folder in Ignition
				// if present.
				KuraPosition position = new KuraPosition();
				position.setAltitude(319);
				position.setHeading(0);
				position.setLatitude(38.83667239);
				position.setLongitude(-94.67176706);
				position.setPrecision(2.0);
				position.setSatellites(8);
				position.setSpeed(0);
				position.setStatus(3);
				position.setTimestamp(new Date());
				payload.setPosition(position);

				//
				// Now publish the EoN Node Birth Certificate.
				// Note that the required "Sequence Number" metric 'seq' needs
				// to
				// be RESET TO A VALUE OF ZERO for the message. The 'timestamp'
				// metric
				// is added into the payload by the Publisher() thread.
				//
				executor.execute(new Publisher("spAv1.0/" + groupId + "/NBIRTH/" + edgeNode, payload));

				//
				// Create the Device BIRTH Certificate now. The tags defined
				// here will appear in a
				// folder hierarchy under the associated Device.
				//
				payload = new KuraPayload();
				payload.setTimestamp(new Date());
				payload = addSeqNum(payload);
				// Create an "Inputs" folder of process variables
				payload.addMetric("Inputs/a", pibrella.getInputPin(PibrellaInput.A).isHigh());
				payload.addMetric("Inputs/b", pibrella.getInputPin(PibrellaInput.B).isHigh());
				payload.addMetric("Inputs/c", pibrella.getInputPin(PibrellaInput.C).isHigh());
				payload.addMetric("Inputs/d", pibrella.getInputPin(PibrellaInput.D).isHigh());
				// Create an "Outputs" folder of process variables
				payload.addMetric("Outputs/e", pibrella.getOutputPin(PibrellaOutput.E).isHigh());
				payload.addMetric("Outputs/f", pibrella.getOutputPin(PibrellaOutput.F).isHigh());
				payload.addMetric("Outputs/g", pibrella.getOutputPin(PibrellaOutput.G).isHigh());
				payload.addMetric("Outputs/h", pibrella.getOutputPin(PibrellaOutput.H).isHigh());
				// Create an additional folder under "Outputs" called "LEDs"
				payload.addMetric("Outputs/LEDs/green", pibrella.getOutputPin(PibrellaOutput.LED_GREEN).isHigh());
				payload.addMetric("Outputs/LEDs/red", pibrella.getOutputPin(PibrellaOutput.LED_RED).isHigh());
				payload.addMetric("Outputs/LEDs/yellow", pibrella.getOutputPin(PibrellaOutput.LED_YELLOW).isHigh());
				// Place the button process variables at the root level of the
				// tag hierarchy
				payload.addMetric("button", pibrella.getInputPin(PibrellaInput.Button).isHigh());
				payload.addMetric("button count", buttonCounter);
				payload.addMetric("button count setpoint", buttonCounterSetpoint);
				payload.addMetric("buzzer", false);

				//
				// Add some properties to the Properties folder
				//
				payload.addMetric("Properties/dev_type", "Pibrella");
				payload.addMetric("Properties/hw_version", "3.0.1");

				// Publish the Device BIRTH Certificate now
				executor.execute(new Publisher("spAv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId, payload));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Used to add the birth/death sequence number
	private KuraPayload addBdSeqNum(KuraPayload payload) throws Exception {
		if (payload == null) {
			payload = new KuraPayload();
		}
		if (bdSeq == 256) {
			bdSeq = 0;
		}
		payload.addMetric("bdSeq", bdSeq);
		bdSeq++;
		return payload;
	}

	// Used to add the sequence number
	private KuraPayload addSeqNum(KuraPayload payload) throws Exception {
		if (payload == null) {
			payload = new KuraPayload();
		}
		if (seq == 256) {
			seq = 0;
		}
		payload.addMetric("seq", seq);
		seq++;
		return payload;
	}

	public void connectionLost(Throwable cause) {
		System.out.println("The MQTT Connection was lost!");
	}

	/**
	 * Based on our subscriptions to the MQTT Server, the messageArrived() callback is
	 * called on all arriving MQTT messages. Based on the Sparkplug Topic Namespace, 
	 * each message is parsed and an appropriate action is taken.
	 * 
	 */
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("Message Arrived on topic " + topic);

		// Initialize the outbound payload if required.
		KuraPayload outboundPayload = new KuraPayload();
		outboundPayload.setTimestamp(new Date());
		outboundPayload = addSeqNum(outboundPayload);

		String[] splitTopic = topic.split("/");
		if (splitTopic[0].equals("spAv1.0") && splitTopic[1].equals(groupId) && splitTopic[2].equals("NCMD")
				&& splitTopic[3].equals(edgeNode)) {
			CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
			KuraPayload inboundPayload = decoder.buildFromByteArray();

			Iterator<Entry<String, Object>> metrics = inboundPayload.metrics().entrySet().iterator();
			while (metrics.hasNext()) {
				Entry<String, Object> entry = metrics.next();
				System.out.println("Metric: " + entry.getKey() + " :: " + entry.getValue());
			}

			if (inboundPayload.getMetric("Node Control/Rebirth") != null
					&& (Boolean) inboundPayload.getMetric("Node Control/Rebirth") == true) {
				publishBirth();
			}
			if (inboundPayload.getMetric("Node Control/Reboot") != null
					&& (Boolean) inboundPayload.getMetric("Node Control/Reboot") == true) {
				System.out.println("Received a Reboot command.");
			}
			if (inboundPayload.getMetric("Node Control/Next Server") != null
					&& (Boolean) inboundPayload.getMetric("Node Control/Next Server") == true) {
				System.out.println("Received a Next Server command.");
			}
			if (inboundPayload.getMetric("Node Control/Scan Rate ms") != null) {
				scanRateMs = (Integer) inboundPayload.getMetric("Node Control/Scan Rate ms");
				if (scanRateMs < 100) {
					// Limit Scan Rate to a minimum of 100ms
					scanRateMs = 100;
				}
				outboundPayload.addMetric("Node Control/Scan Rate ms", scanRateMs);
				// Publish the message in a new thread
				synchronized (lock) {
					executor.execute(new Publisher("spAv1.0/" + groupId + "/NDATA/" + edgeNode, outboundPayload));
				}
			}
		} else if (splitTopic[0].equals("spAv1.0") && splitTopic[1].equals(groupId) && splitTopic[2].equals("DCMD")
				&& splitTopic[3].equals(edgeNode)) {
			synchronized (lock) {
				System.out.println("Command recevied for device: " + splitTopic[4] + " on topic: " + topic);

				// Get the incoming metric key and value
				CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
				KuraPayload inboundPayload = decoder.buildFromByteArray();

				Iterator<Entry<String, Object>> metrics = inboundPayload.metrics().entrySet().iterator();
				while (metrics.hasNext()) {
					Entry<String, Object> entry = metrics.next();
					System.out.println("Metric: " + entry.getKey() + " :: " + entry.getValue());
				}

				if (inboundPayload.getMetric("Outputs/e") != null) {
					pibrella.getOutputPin(PibrellaOutput.E).setState((Boolean) inboundPayload.getMetric("Outputs/e"));
					outboundPayload.addMetric("Outputs/e", pibrella.getOutputPin(PibrellaOutput.E).isHigh());
				}
				if (inboundPayload.getMetric("Outputs/f") != null) {
					pibrella.getOutputPin(PibrellaOutput.F).setState((Boolean) inboundPayload.getMetric("Outputs/f"));
					outboundPayload.addMetric("Outputs/f", pibrella.getOutputPin(PibrellaOutput.F).isHigh());
				}
				if (inboundPayload.getMetric("Outputs/g") != null) {
					pibrella.getOutputPin(PibrellaOutput.G).setState((Boolean) inboundPayload.getMetric("Outputs/g"));
					outboundPayload.addMetric("Outputs/g", pibrella.getOutputPin(PibrellaOutput.G).isHigh());
				}
				if (inboundPayload.getMetric("Outputs/h") != null) {
					pibrella.getOutputPin(PibrellaOutput.H).setState((Boolean) inboundPayload.getMetric("Outputs/h"));
					outboundPayload.addMetric("Outputs/h", pibrella.getOutputPin(PibrellaOutput.H).isHigh());
				}
				if (inboundPayload.getMetric("Outputs/LEDs/green") != null) {
					if ((Boolean) inboundPayload.getMetric("Outputs/LEDs/green") == true) {
						pibrella.ledGreen().on();
					} else {
						pibrella.ledGreen().off();
					}
					outboundPayload.addMetric("Outputs/LEDs/green", pibrella.ledGreen().isOn());
				}
				if (inboundPayload.getMetric("Outputs/LEDs/red") != null) {
					if ((Boolean) inboundPayload.getMetric("Outputs/LEDs/red") == true) {
						pibrella.ledRed().on();
					} else {
						pibrella.ledRed().off();
					}
					outboundPayload.addMetric("Outputs/LEDs/red", pibrella.ledRed().isOn());
				}
				if (inboundPayload.getMetric("Outputs/LEDs/yellow") != null) {
					if ((Boolean) inboundPayload.getMetric("Outputs/LEDs/yellow") == true) {
						pibrella.ledYellow().on();
					} else {
						pibrella.ledYellow().off();
					}
					outboundPayload.addMetric("Outputs/LEDs/yellow", pibrella.ledYellow().isOn());
				}
				if (inboundPayload.getMetric("button count setpoint") != null) {
					buttonCounterSetpoint = (Integer) inboundPayload.getMetric("button count setpoint");
					outboundPayload.addMetric("button count setpoint", buttonCounterSetpoint);
				}
				if (inboundPayload.getMetric("buzzer") != null) {
					pibrella.getBuzzer().buzz(100, 2000);
				}

				// Publish the message in a new thread
				executor.execute(
						new Publisher("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, outboundPayload));
			}
		}
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		// System.out.println("Published message: " + token);
	}

	private class Publisher implements Runnable {

		private String topic;
		private KuraPayload outboundPayload;

		public Publisher(String topic, KuraPayload outboundPayload) {
			this.topic = topic;
			this.outboundPayload = outboundPayload;
		}

		public void run() {
			try {
				CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
				client.publish(topic, encoder.getBytes(), 0, false);
			} catch (MqttPersistenceException e) {
				e.printStackTrace();
			} catch (MqttException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void createPibrellaListeners() {
		pibrella.button().addListener(new ButtonStateChangeListener() {
			public void onStateChange(ButtonStateChangeEvent event) {
				try {
					synchronized (lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if (event.getButton().getState() == ButtonState.PRESSED) {
							outboundPayload.addMetric("button", true);
							buttonCounter++;
							if (buttonCounter > buttonCounterSetpoint) {
								buttonCounter = 0;
							}
							outboundPayload.addMetric("button count", buttonCounter);
						} else {
							outboundPayload.addMetric("button", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(),
								0, false);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		pibrella.inputA().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized (lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if (event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("Inputs/a", true);
						} else {
							outboundPayload.addMetric("Inputs/a", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(),
								0, false);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		pibrella.inputB().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized (lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if (event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("Inputs/b", true);
						} else {
							outboundPayload.addMetric("Inputs/b", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(),
								0, false);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		pibrella.inputC().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized (lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if (event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("Inputs/c", true);
						} else {
							outboundPayload.addMetric("Inputs/c", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(),
								0, false);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		pibrella.inputD().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized (lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if (event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("Inputs/d", true);
						} else {
							outboundPayload.addMetric("Inputs/d", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(),
								0, false);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
}
