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

public class SparkplugExample implements MqttCallback {

	// HW/SW versions
	private static final String HW_VERSION = "Emulated Hardware";
	private static final String SW_VERSION = "v1.0.0";

	// Configuration
	private String serverUrl = "tcp://localhost:1883";
	private String groupId = "Sparkplug Devices";
	private String edgeNode = "Java Edge Node";
	private String deviceId = "Emulated Device";
	private String clientId = "javaSimpleEdgeNode";
	private String username = "admin";
	private String password = "changeme";
	private long PUBLISH_PERIOD = 5000;					// Publish period in milliseconds
	private ExecutorService executor;
	private MqttClient client;
	
	private int bdSeq = 0;
	private int seq = 0;
	
	private Object seqLock = new Object();
	
	public static void main(String[] args) {
		SparkplugExample example = new SparkplugExample();
		example.run();
	}
	
	public void run() {
		try {
			// Random generator and thread pool for outgoing published messages
			Random random = new Random();
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
			options.setWill("spAv1.0/" + groupId + "/NDEATH/" + edgeNode, deathEncoder.getBytes(), 0, false);
			client = new MqttClient(serverUrl, clientId);
			client.setTimeToWait(2000);						// short timeout on failure to connect
			client.connect(options);
			client.setCallback(this);
			
			// Subscribe to control/command messages for both the edge of network node and the attached devices
			client.subscribe("spAv1.0/" + groupId + "/NCMD/" + edgeNode + "/#", 0);
			client.subscribe("spAv1.0/" + groupId + "/DCMD/" + edgeNode + "/#", 0);
			
			// Publish the Birth certificate
			publishBirth();			
			
			// Loop 100 times publishing data every PUBLISH_PERIOD
			for(int i=0; i<100; i++) {
				Thread.sleep(PUBLISH_PERIOD);

				synchronized(seqLock) {
					// Create the payload and add some metrics
					KuraPayload payload = new KuraPayload();
					payload.addMetric("my_boolean", random.nextBoolean());
					payload.addMetric("my_double", random.nextDouble());
					payload.addMetric("my_float", random.nextFloat());
					payload.addMetric("my_int", random.nextInt(100));
					payload.addMetric("my_long", random.nextLong());
					
					System.out.println("Publishing updated values");
					payload = addSeqNum(payload);
					CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(payload);
					client.publish("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
				}
			}

			// Close the connection
			client.disconnect();			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void publishBirth() {
		try {
			synchronized(seqLock) {
				Random random = new Random();
				
				// Create the BIRTH payload and set the position and other metrics
				KuraPayload payload = new KuraPayload();
				payload.setTimestamp(new Date());
				payload.addMetric("bdSeq", bdSeq);
				seq = 0;									// Since this is a birth - reset the seq number
				payload = addSeqNum(payload);
				
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
				payload.setPosition(position);
				
				payload.addMetric("Node Control/Rebirth", false);
				
				System.out.println("Publishing Edge Node Birth with " + payload.getMetric("seq"));
				executor.execute(new Publisher("spAv1.0/" + groupId + "/NBIRTH/" + edgeNode, payload));
	
				// Create the payload and add some metrics
				payload = new KuraPayload();
				payload.setTimestamp(new Date());
				payload = addSeqNum(payload);
				payload.addMetric("my_boolean", random.nextBoolean());
				payload.addMetric("my_double", random.nextDouble());
				payload.addMetric("my_float", random.nextFloat());
				payload.addMetric("my_int", random.nextInt(100));
				payload.addMetric("my_long", random.nextLong());
	
				// Only do this once to set up the inputs and outputs
				payload.addMetric("Inputs/0", true);
				payload.addMetric("Inputs/1", 0);
				payload.addMetric("Inputs/2", 1.23);
				payload.addMetric("Outputs/0", true);
				payload.addMetric("Outputs/1", 0);
				payload.addMetric("Outputs/2", 1.23);
	
				// Add some properties
				payload.addMetric("Properties/hw_version", HW_VERSION);
				payload.addMetric("Properties/sw_version", SW_VERSION);
	
				System.out.println("Publishing Device Birth");
				executor.execute(new Publisher("spAv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId, payload));
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
		
		CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
		KuraPayload inboundPayload = decoder.buildFromByteArray();
		
		// Debug
		Iterator<Entry<String, Object>> it = inboundPayload.metrics().entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, Object> entry = it.next();
			System.out.println("Metric " + entry.getKey() + "=" + entry.getValue());
		}
		
		String[] splitTopic = topic.split("/");
		if(splitTopic[0].equals("spAv1.0") && 
				splitTopic[1].equals(groupId) &&
				splitTopic[2].equals("NCMD") && 
				splitTopic[3].equals(edgeNode)) {
			if(inboundPayload.getMetric("Rebirth") != null && (Boolean)inboundPayload.getMetric("Rebirth") == true) {
				publishBirth();
			}
		} else if(splitTopic[0].equals("spAv1.0") && 
				splitTopic[1].equals(groupId) &&
				splitTopic[2].equals("DCMD") && 
				splitTopic[3].equals(edgeNode)) {
			System.out.println("Command recevied for device " + splitTopic[4]);
			
			// Get the incoming metric key and value
			// Pretend Outputs/0 is tied to Inputs/0 and Outputs/2 is tied to Inputs/1 and Outputs/2 is tied to Inputs/2
			KuraPayload outboundPayload = new KuraPayload();
			outboundPayload.setTimestamp(new Date());
			outboundPayload = addSeqNum(outboundPayload);
			if(inboundPayload.getMetric("Outputs/0") != null) {
				System.out.println("Outputs/0: " + inboundPayload.getMetric("Outputs/0"));
				outboundPayload.addMetric("Inputs/0", inboundPayload.getMetric("Outputs/0"));
				outboundPayload.addMetric("Outputs/0", inboundPayload.getMetric("Outputs/0"));
				System.out.println("Publishing updated value for Inputs/0 " + inboundPayload.getMetric("Outputs/0"));
			} else if(inboundPayload.getMetric("Outputs/1") != null) {
				System.out.println("Output1: " + inboundPayload.getMetric("Outputs/1"));
				outboundPayload.addMetric("Inputs/1", inboundPayload.getMetric("Outputs/1"));
				outboundPayload.addMetric("Outputs/1", inboundPayload.getMetric("Outputs/1"));
				System.out.println("Publishing updated value for Inputs/1 " + inboundPayload.getMetric("Outputs/1"));
			} else if(inboundPayload.getMetric("Outputs/2") != null) {
				System.out.println("Output2: " + inboundPayload.getMetric("Outputs/2"));
				outboundPayload.addMetric("Inputs/2", inboundPayload.getMetric("Outputs/2"));
				outboundPayload.addMetric("Outputs/2", inboundPayload.getMetric("Outputs/2"));
				System.out.println("Publishing updated value for Inputs/2 " + inboundPayload.getMetric("Outputs/2"));
			}

			// Publish the message in a new thread
			executor.execute(new Publisher("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, outboundPayload));
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
				outboundPayload.setTimestamp(new Date());
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
}
