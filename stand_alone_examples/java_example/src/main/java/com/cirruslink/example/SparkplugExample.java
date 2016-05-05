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
			options.setWill("spv1.0/" + groupId + "/NDEATH/" + edgeNode, deathEncoder.getBytes(), 0, false);
			client = new MqttClient(serverUrl, clientId);
			client.connect(options);
			client.setCallback(this);
			
			// Subscribe to control/command messages for both the edge of network node and the attached devices
			client.subscribe("spv1.0/" + groupId + "/NCMD/" + edgeNode + "/#", 0);
			client.subscribe("spv1.0/" + groupId + "/DCMD/" + edgeNode + "/#", 0);
			
			// Publish the Birth certificate
			publishBirth();			
			
			// Loop 100 times publishing data every PUBLISH_PERIOD
			for(int i=0; i<100; i++) {
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
				client.publish("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, encoder.getBytes(), 0, false);
				
				Thread.sleep(PUBLISH_PERIOD);
			}

			// Close the connection
			client.disconnect();			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void publishBirth() {
		try {
			Random random = new Random();
			
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
			System.out.println("Publishing Edge Node Birth");
			executor.execute(new Publisher("spv1.0/" + groupId + "/NBIRTH/" + edgeNode, payload));

			// Create the payload and add some metrics
			payload = new KuraPayload();
			payload.addMetric("my_boolean", random.nextBoolean());
			payload.addMetric("my_double", random.nextDouble());
			payload.addMetric("my_float", random.nextFloat());
			payload.addMetric("my_int", random.nextInt(100));
			payload.addMetric("my_long", random.nextLong());

			// Only do this once to set up the inputs and outputs
			payload.addMetric("input0", true);
			payload.addMetric("input1", 0);
			payload.addMetric("input2", 1.23);
			payload.addMetric("output0", true);
			payload.addMetric("output1", 0);
			payload.addMetric("output2", 1.23);

			// We need to publish the device's birth certificate with all known data and parameters
			KuraPayload totalPayload = new KuraPayload();
			totalPayload.setTimestamp(new Date());
			totalPayload = addSeqNum(totalPayload);

			KuraPayload parameterPayload = new KuraPayload();
			parameterPayload.addMetric("hw_version", HW_VERSION);
			parameterPayload.addMetric("sw_version", SW_VERSION);
			CloudPayloadEncoder encoder = new CloudPayloadProtoBufEncoderImpl(parameterPayload);
			totalPayload.addMetric("device_parameters", encoder.getBytes());

			// Add the initial I/O states
			encoder = new CloudPayloadProtoBufEncoderImpl(payload);
			totalPayload.addMetric("pv_map", encoder.getBytes());

			System.out.println("Publishing Device Birth");
			executor.execute(new Publisher("spv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/" + deviceId, totalPayload));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	// Used to add the birth/death sequence number
	private KuraPayload addBdSeqNum(KuraPayload payload) throws Exception {
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
	private KuraPayload addSeqNum(KuraPayload payload) throws Exception {
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
				splitTopic[2].equals("NCMD") && 
				splitTopic[3].equals(edgeNode)) {
			CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
			KuraPayload inboundPayload = decoder.buildFromByteArray();
			if(inboundPayload.getMetric("Rebirth") != null && (Boolean)inboundPayload.getMetric("Rebirth") == true) {
				publishBirth();
			}
		} else if(splitTopic[0].equals("spv1.0") && 
				splitTopic[1].equals(groupId) &&
				splitTopic[2].equals("DCMD") && 
				splitTopic[3].equals(edgeNode)) {
			System.out.println("Command recevied for device " + splitTopic[4]);
			
			// Get the incoming metric key and value
			CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
			KuraPayload inboundPayload = decoder.buildFromByteArray();
			
			// Pretend output0 is tied to input0 and output1 is tied to input1 and output2 is tied to input2
			KuraPayload outboundPayload = new KuraPayload();
			if(inboundPayload.getMetric("output0") != null) {
				System.out.println("Output0: " + inboundPayload.getMetric("output0"));
				outboundPayload.addMetric("input0", inboundPayload.getMetric("output0"));
				outboundPayload.addMetric("output0", inboundPayload.getMetric("output0"));
				System.out.println("Publishing updated value for input0 " + inboundPayload.getMetric("output0"));
			} else if(inboundPayload.getMetric("output1") != null) {
				System.out.println("Output1: " + inboundPayload.getMetric("output1"));
				outboundPayload.addMetric("input1", inboundPayload.getMetric("output1"));
				outboundPayload.addMetric("output1", inboundPayload.getMetric("output1"));
				System.out.println("Publishing updated value for input1 " + inboundPayload.getMetric("output1"));
			} else if(inboundPayload.getMetric("output2") != null) {
				System.out.println("Output2: " + inboundPayload.getMetric("output2"));
				outboundPayload.addMetric("input2", inboundPayload.getMetric("output2"));
				outboundPayload.addMetric("output2", inboundPayload.getMetric("output2"));
				System.out.println("Publishing updated value for input2 " + inboundPayload.getMetric("output2"));
			}

			// Publish the message in a new thread
			executor.execute(new Publisher("spv1.0/" + groupId + "/DDATA/" + edgeNode + "/" + deviceId, outboundPayload));
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
				outboundPayload = addSeqNum(outboundPayload);
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
