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

public class SparkplugRaspberryPiExample implements MqttCallback {
	
	private static final Pibrella pibrella = new PibrellaDevice();
	
	// HW/SW versions
	private static final String HW_VERSION = "Raspberry Pi 2 model B";
	private static final String SW_VERSION = "v1.0.0";

	// Configuration
	private String serverUrl = "tcp://192.168.1.84:1883";			// Change to point to your MQTT Server
	private String groupId = "Sparkplug Devices";
	private String edgeNode = "Java Raspberry Pi";
	private String deviceId = "Pibrella";
	private String clientId = "javaPibrellaClientId";
	private String username = "admin";
	private String password = "changeme";
	private ExecutorService executor;
	private MqttClient client;
	
	private int bdSeq = 0;
	private int seq = 0;
	
	private Object lock = new Object();

	public static void main(String[] args) {
		SparkplugRaspberryPiExample example = new SparkplugRaspberryPiExample();
		example.run();
	}
	
	public void run() {
		try {
			// Create the Pibrella listeners
			createPibrellaListeners();
			
			// Thread pool for outgoing published messages
			executor = Executors.newFixedThreadPool(1);
			
			// Build up DEATH payload - note DEATH payloads don't have a regular sequence number
			KuraPayload deathPayload = new KuraPayload();
			deathPayload.setTimestamp(new Date());
			deathPayload = addBdSeqNum(deathPayload);
			CloudPayloadEncoder deathEncoder = new CloudPayloadProtoBufEncoderImpl(deathPayload);
			
			// Connect to the MQTT Server
			MqttConnectOptions options = new MqttConnectOptions();
			options.setCleanSession(true);
			options.setConnectionTimeout(30);
			options.setKeepAliveInterval(30);
			options.setUserName(username);
			options.setPassword(password.toCharArray());
			options.setWill("spv1.0/" + groupId + "/NDEATH/" + edgeNode, deathEncoder.getBytes(), 0, false);
			client = new MqttClient(serverUrl, clientId);
			client.setTimeToWait(2000);						// short timeout on failure to connect
			client.connect(options);
			client.setCallback(this);
			
			// Subscribe to control/command messages for both the edge of network node and the attached devices
			client.subscribe("spv1.0/" + groupId + "/NCMD/" + edgeNode + "/#", 0);
			client.subscribe("spv1.0/" + groupId + "/DCMD/" + edgeNode + "/#", 0);
			
			publishBirth();
			
			// Wait for 'ctrl c' to exit
			while(true) {
				Thread.sleep(1000);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void publishBirth() {
		try {
			synchronized(lock) {
				// Create the BIRTH payload and set the position and other metrics
				KuraPayload payload = new KuraPayload();
				payload.setTimestamp(new Date());
				payload.addMetric("bdSeq", bdSeq);
				seq = 0;									// Since this is a birth - reset the seq number
				payload = addSeqNum(payload);
				
				// Create the position
				payload.addMetric("Position/Altitude", 319);
				payload.addMetric("Position/Heading", 0);
				payload.addMetric("Position/Latitude", 38.83667239);
				payload.addMetric("Position/Longitude", -94.67176706);
				payload.addMetric("Position/Precision", 2.0);
				payload.addMetric("Position/Satellites", 8);
				payload.addMetric("Position/Speed", 0);
				payload.addMetric("Position/Status", 3);
				
				payload.addMetric("Node Control/Rebirth", false);
				
				executor.execute(new Publisher("spv1.0/" + groupId + "/NBIRTH/" + edgeNode, payload));

				// Create the Device BIRTH
				payload = new KuraPayload();
				payload.addMetric("input_a", pibrella.getInputPin(PibrellaInput.A).isHigh());
				payload.addMetric("input_b", pibrella.getInputPin(PibrellaInput.B).isHigh());
				payload.addMetric("input_c", pibrella.getInputPin(PibrellaInput.C).isHigh());
				payload.addMetric("input_d", pibrella.getInputPin(PibrellaInput.D).isHigh());
				payload.addMetric("Outputs/e", pibrella.getOutputPin(PibrellaOutput.E).isHigh());
				payload.addMetric("Outputs/f", pibrella.getOutputPin(PibrellaOutput.F).isHigh());
				payload.addMetric("Outputs/g", pibrella.getOutputPin(PibrellaOutput.G).isHigh());
				payload.addMetric("Outputs/h", pibrella.getOutputPin(PibrellaOutput.H).isHigh());
				payload.addMetric("Outputs/leds/green", pibrella.getOutputPin(PibrellaOutput.LED_GREEN).isHigh());
				payload.addMetric("Outputs/leds/red", pibrella.getOutputPin(PibrellaOutput.LED_RED).isHigh());
				payload.addMetric("Outputs/leds/yellow", pibrella.getOutputPin(PibrellaOutput.LED_YELLOW).isHigh());
				payload.addMetric("button", pibrella.getInputPin(PibrellaInput.Button).isHigh());
				payload.addMetric("buzzer", false);

				// We need to publish the device's birth certificate with all known data and parameters
				KuraPayload totalPayload = new KuraPayload();
				totalPayload.setTimestamp(new Date());
				totalPayload = addSeqNum(totalPayload);

				KuraPayload parameterPayload = new KuraPayload();
				parameterPayload.addMetric("Properties/hw_version", HW_VERSION);
				parameterPayload.addMetric("Properties/sw_version", SW_VERSION);
				CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(parameterPayload);
				totalPayload.addMetric("device_parameters", encoder.getBytes());

				// Add the initial I/O states
				encoder = new CloudPayloadProtoBufEncoderImpl(payload);
				totalPayload.addMetric("pv_map", encoder.getBytes());

				// Publish the Device BIRTH
				executor.execute(new Publisher("spv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId, totalPayload));
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	// Used to add the birth/death sequence number
	private KuraPayload addBdSeqNum(KuraPayload payload) throws Exception {
		if(payload == null) {
			payload = new KuraPayload();
		}
		if(bdSeq == 256) {
			bdSeq = 0;
		}
		payload.addMetric("bdSeq", bdSeq);
		bdSeq++;
		return payload;
	}
	
	// Used to add the sequence number
	private KuraPayload addSeqNum(KuraPayload payload) throws Exception {
		if(payload == null) {
			payload = new KuraPayload();
		}
		if(seq == 256) {
			seq = 0;
		}
		payload.addMetric("seq", seq);
		seq++;
		return payload;
	}

	public void connectionLost(Throwable cause) {
		System.out.println("The MQTT Connection was lost!");
	}

	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("Message Arrived on topic " + topic);
		
		String[] splitTopic = topic.split("/");
		if(splitTopic[0].equals("spv1.0") && 
				splitTopic[1].equals(groupId) &&
				splitTopic[2].equals("NCMD") && 
				splitTopic[3].equals(edgeNode)) {
			CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
			KuraPayload inboundPayload = decoder.buildFromByteArray();

			Iterator<Entry<String, Object>> metrics = inboundPayload.metrics().entrySet().iterator();
			while(metrics.hasNext()) {
				Entry<String, Object> entry = metrics.next();
				System.out.println("Metric: " + entry.getKey() + " :: " + entry.getValue());
			}

			if(inboundPayload.getMetric("Node Control/Rebirth") != null && (Boolean)inboundPayload.getMetric("Node Control/Rebirth") == true) {
				publishBirth();
			}
		} else if(splitTopic[0].equals("spv1.0") && 
				splitTopic[1].equals(groupId) &&
				splitTopic[2].equals("DCMD") && 
				splitTopic[3].equals(edgeNode)) {
			synchronized(lock) {
				System.out.println("Command recevied for device: " + splitTopic[4] + " on topic: " + topic);

				// Get the incoming metric key and value
				CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
				KuraPayload inboundPayload = decoder.buildFromByteArray();
				
				Iterator<Entry<String, Object>> metrics = inboundPayload.metrics().entrySet().iterator();
				while(metrics.hasNext()) {
					Entry<String, Object> entry = metrics.next();
					System.out.println("Metric: " + entry.getKey() + " :: " + entry.getValue());
				}

				// Initialize the outbound payload
				KuraPayload outboundPayload = new KuraPayload();
				outboundPayload.setTimestamp(new Date());
				outboundPayload = addSeqNum(outboundPayload);

				if(inboundPayload.getMetric("Outputs/e") != null) {
					pibrella.getOutputPin(PibrellaOutput.E).setState((Boolean)inboundPayload.getMetric("Outputs/e"));
					outboundPayload.addMetric("Outputs/e", pibrella.getOutputPin(PibrellaOutput.E).isHigh());
				}
				if(inboundPayload.getMetric("Outputs/f") != null) {
					pibrella.getOutputPin(PibrellaOutput.F).setState((Boolean)inboundPayload.getMetric("Outputs/f"));
					outboundPayload.addMetric("Outputs/f", pibrella.getOutputPin(PibrellaOutput.F).isHigh());
				}
				if(inboundPayload.getMetric("Outputs/g") != null) {
					pibrella.getOutputPin(PibrellaOutput.G).setState((Boolean)inboundPayload.getMetric("Outputs/g"));
					outboundPayload.addMetric("Outputs/g", pibrella.getOutputPin(PibrellaOutput.G).isHigh());
				}
				if(inboundPayload.getMetric("Outputs/h") != null) {
					pibrella.getOutputPin(PibrellaOutput.H).setState((Boolean)inboundPayload.getMetric("Outputs/h"));
					outboundPayload.addMetric("Outputs/h", pibrella.getOutputPin(PibrellaOutput.H).isHigh());
				}
				if(inboundPayload.getMetric("Outputs/leds/green") != null) {
					if((Boolean)inboundPayload.getMetric("Outputs/leds/green") == true) {
						pibrella.ledGreen().on();
					} else {
						pibrella.ledGreen().off();
					}
					outboundPayload.addMetric("Outputs/leds/green", pibrella.ledGreen().isOn());
				}
				if(inboundPayload.getMetric("Outputs/leds/red") != null) {
					if((Boolean)inboundPayload.getMetric("Outputs/leds/red") == true) {
						pibrella.ledRed().on();
					} else {
						pibrella.ledRed().off();
					}
					outboundPayload.addMetric("Outputs/leds/red", pibrella.ledRed().isOn());
				}
				if(inboundPayload.getMetric("Outputs/leds/yellow") != null) {
					if((Boolean)inboundPayload.getMetric("Outputs/leds/yellow") == true) {
						pibrella.ledYellow().on();
					} else {
						pibrella.ledYellow().off();
					}
					outboundPayload.addMetric("Outputs/leds/yellow", pibrella.ledYellow().isOn());
				}
				if(inboundPayload.getMetric("buzzer") != null) {
					pibrella.getBuzzer().buzz(100, 2000);
				}

				// Publish the message in a new thread
				executor.execute(new Publisher("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, outboundPayload));
			}
		}
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		//System.out.println("Published message: " + token);
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
					synchronized(lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if(event.getButton().getState() == ButtonState.PRESSED) {
							outboundPayload.addMetric("button", true);
						} else {
							outboundPayload.addMetric("button", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
        
        pibrella.inputA().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized(lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if(event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("input_a", true);
						} else {
							outboundPayload.addMetric("input_a", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
        });
        
        pibrella.inputB().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized(lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if(event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("input_b", true);
						} else {
							outboundPayload.addMetric("input_b", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
        });

        pibrella.inputC().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized(lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if(event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("input_c", true);
						} else {
							outboundPayload.addMetric("input_c", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
        });
        
        pibrella.inputD().addListener(new GpioPinListenerDigital() {
			public void handleGpioPinDigitalStateChangeEvent(GpioPinDigitalStateChangeEvent event) {
				try {
					synchronized(lock) {
						KuraPayload outboundPayload = new KuraPayload();
						outboundPayload.setTimestamp(new Date());
						outboundPayload = addSeqNum(outboundPayload);
						if(event.getState() == PinState.HIGH) {
							outboundPayload.addMetric("input_d", true);
						} else {
							outboundPayload.addMetric("input_d", false);
						}
						CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
						client.publish("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
        });
	}
}
