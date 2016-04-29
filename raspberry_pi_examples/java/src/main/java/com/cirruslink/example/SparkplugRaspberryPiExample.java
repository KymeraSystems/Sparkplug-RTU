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
import java.util.Random;
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
	private String serverUrl = "tcp://192.168.1.1:1883";			// Change to point to your MQTT Server
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

	public static void main(String[] args) {
		SparkplugRaspberryPiExample example = new SparkplugRaspberryPiExample();
		example.run();
	}
	
	public void run() {
		try {
			// Create the Pibrella listeners
			createPibrellaListeners();
			
			// Random generator and thread pool for outgoing published messages
			Random random = new Random();
			executor = Executors.newFixedThreadPool(1);
			
			// Flag for first run to denote subnode state
			boolean deviceOnline = false;
			
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
			client.connect(options);
			client.setCallback(this);
			
			// Subscribe to control/command messages for both the edge of network node and the attached devices
			client.subscribe("spv1.0/" + groupId + "/NCMD/" + edgeNode + "/#", 0);
			client.subscribe("spv1.0/" + groupId + "/DCMD/" + edgeNode + "/#", 0);
			
			// Create the position for the Kura payload
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
			
			// Create the BIRTH payload and set the position and other metrics
			KuraPayload payload = new KuraPayload();
			payload.setTimestamp(new Date());
			payload.addMetric("bdSeq", bdSeq);
			payload = addSeqNum(payload);
			payload.setPosition(position);
			CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(payload);
			client.publish("spv1.0/" + groupId + "/NBIRTH/" + edgeNode, encoder.getBytes(), 0, false);
			
			// Create the Device BIRTH
			payload = new KuraPayload();
			payload.addMetric("input_a", pibrella.getInputPin(PibrellaInput.A).isHigh());
			payload.addMetric("input_b", pibrella.getInputPin(PibrellaInput.B).isHigh());
			payload.addMetric("input_c", pibrella.getInputPin(PibrellaInput.C).isHigh());
			payload.addMetric("input_d", pibrella.getInputPin(PibrellaInput.D).isHigh());
			payload.addMetric("output_e", pibrella.getOutputPin(PibrellaOutput.E).isHigh());
			payload.addMetric("output_f", pibrella.getOutputPin(PibrellaOutput.F).isHigh());
			payload.addMetric("output_g", pibrella.getOutputPin(PibrellaOutput.G).isHigh());
			payload.addMetric("output_h", pibrella.getOutputPin(PibrellaOutput.H).isHigh());
			payload.addMetric("led_green", pibrella.getOutputPin(PibrellaOutput.LED_GREEN).isHigh());
			payload.addMetric("led_red", pibrella.getOutputPin(PibrellaOutput.LED_RED).isHigh());
			payload.addMetric("led_yellow", pibrella.getOutputPin(PibrellaOutput.LED_YELLOW).isHigh());
			payload.addMetric("button", pibrella.getInputPin(PibrellaInput.Button).isHigh());
			payload.addMetric("buzzer", false);
			
			// We need to publish the device's birth certificate with all known data and parameters
			KuraPayload totalPayload = new KuraPayload();
			totalPayload.setTimestamp(new Date());
			totalPayload = addSeqNum(totalPayload);
			totalPayload.setTimestamp(new Date());
			
			KuraPayload parameterPayload = new KuraPayload();
			parameterPayload.addMetric("hw_version", HW_VERSION);
			parameterPayload.addMetric("sw_version", SW_VERSION);
			encoder = new CloudPayloadProtoBufEncoderImpl(parameterPayload);
			totalPayload.addMetric("device_parameters", encoder.getBytes());
			
			// Add the initial I/O states
			encoder = new CloudPayloadProtoBufEncoderImpl(payload);
			totalPayload.addMetric("pv_map", encoder.getBytes());
			
			// Publish the Device BIRTH
			encoder = new CloudPayloadProtoBufEncoderImpl(totalPayload);
			client.publish("spv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
			
			// Wait for 'ctrl c' to exit
			while(true) {
				Thread.sleep(1000);
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	// Used to add the birth/death sequence number
	public KuraPayload addBdSeqNum(KuraPayload payload) throws Exception {
		if(payload == null) {
			payload = new KuraPayload();
		}
		if(bdSeq == 255) {
			bdSeq = 0;
		}
		payload.addMetric("bdSeq", bdSeq++);
		return payload;
	}
	
	// Used to add the sequence number
	public KuraPayload addSeqNum(KuraPayload payload) throws Exception {
		if(payload == null) {
			payload = new KuraPayload();
		}
		if(seq == 255) {
			seq = 0;
		}
		payload.addMetric("seq", seq++);
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
				splitTopic[2].equals("DCMD") && 
				splitTopic[3].equals(edgeNode)) {
			System.out.println("Command recevied for device " + splitTopic[4]);
			
			// Get the incoming metric key and value
			CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
			KuraPayload inboundPayload = decoder.buildFromByteArray();
	
			// Initialize the outbound payload
			KuraPayload outboundPayload = new KuraPayload();
	
			if(inboundPayload.getMetric("output_e") != null) {
				pibrella.getOutputPin(PibrellaOutput.E).setState((Boolean)inboundPayload.getMetric("output_e"));
				outboundPayload.addMetric("output_e", pibrella.getOutputPin(PibrellaOutput.E).isHigh());
			}
			if(inboundPayload.getMetric("output_f") != null) {
				pibrella.getOutputPin(PibrellaOutput.F).setState((Boolean)inboundPayload.getMetric("output_f"));
				outboundPayload.addMetric("output_f", pibrella.getOutputPin(PibrellaOutput.F).isHigh());
			}
			if(inboundPayload.getMetric("output_g") != null) {
				pibrella.getOutputPin(PibrellaOutput.G).setState((Boolean)inboundPayload.getMetric("output_g"));
				outboundPayload.addMetric("output_g", pibrella.getOutputPin(PibrellaOutput.G).isHigh());
			}
			if(inboundPayload.getMetric("output_h") != null) {
				pibrella.getOutputPin(PibrellaOutput.H).setState((Boolean)inboundPayload.getMetric("output_h"));
				outboundPayload.addMetric("output_h", pibrella.getOutputPin(PibrellaOutput.H).isHigh());
			}
			if(inboundPayload.getMetric("led_green") != null) {
				if((Boolean)inboundPayload.getMetric("led_green") == true) {
					pibrella.ledGreen().on();
				} else {
					pibrella.ledGreen().off();
				}
				outboundPayload.addMetric("led_green", pibrella.ledGreen().isOn());
			}
			if(inboundPayload.getMetric("led_red") != null) {
				if((Boolean)inboundPayload.getMetric("led_red") == true) {
					pibrella.ledRed().on();
				} else {
					pibrella.ledRed().off();
				}
				outboundPayload.addMetric("led_red", pibrella.ledRed().isOn());
			}
			if(inboundPayload.getMetric("led_yellow") != null) {
				if((Boolean)inboundPayload.getMetric("led_yellow") == true) {
					pibrella.ledYellow().on();
				} else {
					pibrella.ledYellow().off();
				}
				outboundPayload.addMetric("led_yellow", pibrella.ledYellow().isOn());
			}
			if(inboundPayload.getMetric("buzzer") != null) {
				pibrella.getBuzzer().buzz(100, 2000);
			}
			
			// Publish the message in a new thread
			executor.execute(new Publisher(outboundPayload));
		}
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		//System.out.println("Published message: " + token);
	}
	
	private class Publisher implements Runnable {
		
		private KuraPayload outboundPayload;

		public Publisher(KuraPayload outboundPayload) {
			this.outboundPayload = outboundPayload;
		}
		
		public void run() {
			try {
				outboundPayload.setTimestamp(new Date());
				outboundPayload = addSeqNum(outboundPayload);
				CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(outboundPayload);
				client.publish("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
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
					synchronized(pibrella) {
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
					synchronized(pibrella) {
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
					synchronized(pibrella) {
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
					synchronized(pibrella) {
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
					synchronized(pibrella) {
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
