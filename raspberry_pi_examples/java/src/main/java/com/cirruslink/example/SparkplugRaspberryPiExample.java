/**
 * Copyright (c) 2012, 2016 Cirrus Link Solutions
 * <p>
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * <p>
 * Contributors:
 * Cirrus Link Solutions
 */
package com.cirruslink.example;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cirruslink.example.model.TagValue;
import com.digitalpetri.modbus.requests.ReadCoilsRequest;
import com.digitalpetri.modbus.requests.ReadHoldingRegistersRequest;
import com.digitalpetri.modbus.requests.WriteSingleRegisterRequest;
import com.digitalpetri.modbus.responses.ReadCoilsResponse;
import com.digitalpetri.modbus.responses.ReadHoldingRegistersResponse;
import com.digitalpetri.modbus.responses.WriteSingleRegisterResponse;
import com.digitalpetri.modbus.slave.ModbusTcpSlave;
import com.digitalpetri.modbus.slave.ModbusTcpSlaveConfig;
import com.digitalpetri.modbus.slave.ServiceRequestHandler;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.StdArraySerializers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.SerializationUtils;
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

    private static Pibrella pibrella;
    public static final Random random = new Random();

    private static final HashMap<String, Object> settings = new HashMap();

    // HW/SW versions
    private static final String HW_VERSION = "Raspberry Pi 3 model B";
    private static final String SW_VERSION = "1.0.8";
    private String[] servers;

    // Configuration
    private String serverUrl = "tcp://dv.kymerasystems.com:1883"; // Change to point to
    // your MQTT Server
    private String bindUrl = "localhost";
    private String groupId = "Sparkplug Devices";
    private String edgeNode = null;
    private String deviceId = "Pibrella";
    private String clientId;
    private String username = "admin";
    private String password = "changeme";
    private ExecutorService executor;
    private MqttClient client;
    private String settingsFile = "rtu-config.json";
    public static boolean debug = false;

    // Some control and parameter points for this demo
    private int configChangeCount = 1;
    private int scanRateMs = 1000;
    private long upTimeStart = System.currentTimeMillis();
    private int buttonCounter = 0;
    private int buttonCounterSetpoint = 10;
    private boolean isAPi = false;
    ModbusTcpSlave slave = null;
    private int bdSeq = 0;
    private int seq = 0;

    private Object lock = new Object();
    RTU rtu;
    private String adapter = "wlan0";
    public static HashMap<Short, HashMap<Integer, TagValue>> modbusRegisters = new HashMap<>();
    public static HashMap<Short, HashMap<Integer, TagValue>> modbusCoils = new HashMap<>();

    public SparkplugRaspberryPiExample(String[] args) {

        // if provided, use the first arg as the address to bind to
        if(args.length > 0 && args[0] != null){
            bindUrl = args[0];
        }

        settings.put("meter id", "unknown");
        initializeProps();
        settings.put("meter count", 5);
        settings.put("broker username", "admin");
        settings.put("broker password", "changeme");
        settings.put("latitude", random.nextDouble() * (5.99995) + 53.875221);
        settings.put("longitude", random.nextDouble() * (-19.731444) - 110.157104);
        settings.put("anonymous", false);
        settings.put("updatable", false);
        ArrayList<String> tempServerList = new ArrayList<>();
        tempServerList.add("tcp://192.168.100.60:1883");
        tempServerList.add("tcp://127.0.0.1:1883");
        settings.put("servers", tempServerList);
        settings.put("debug", debug);

        if (new File(settingsFile).exists()) {
            System.out.println("config file exists");
            readConfig();
        }
        settings.put("version", SW_VERSION);
        writeConfig();

        this.edgeNode = ((String) settings.get("meter id"));
        this.clientId = MqttClient.generateClientId();
        // this.isAPi = (Boolean) settings.get("IsAPi");
        int meterCount = (int) settings.get("meter count");
        this.servers = ((ArrayList<String>) settings.get("servers")).toArray(new String[]{});
        this.username = ((String) settings.get("broker username"));
        this.password = ((String) settings.get("broker password"));
        this.debug = ((boolean) settings.get("debug"));

        this.rtu = new RTU(this.edgeNode, meterCount);
        if (this.isAPi) {
            pibrella = new PibrellaDevice();
        }

        setupModbusSlave();
    }

    private void initializeProps() {
        System.out.println("props file doesn't exist");
        String uuid = UUID.randomUUID().toString();
        String id = uuid.substring(uuid.length() - 8);
        settings.put("meter id", id);

        File cpuinfo = new File("/proc/cpuinfo");
        if (cpuinfo.exists()) {
            BufferedReader br = null;
            try {
                FileInputStream fstream = new FileInputStream(cpuinfo);
                br = new BufferedReader(new InputStreamReader(fstream));
                String strLine;
                while ((strLine = br.readLine()) != null) {
                    if (strLine.startsWith("Serial")) {
                        settings.put("meter id", strLine.substring(strLine.length() - 8));
                        this.isAPi = true;
                    }
                }
                try {
                    br.close();
                } catch (IOException localIOException) {
                }
                settings.put("IsAPi", this.isAPi);
                settingsFile = "/home/pi/rtu-config.json";
            } catch (FileNotFoundException localFileNotFoundException) {
            } catch (IOException localIOException2) {
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException localIOException4) {
                    }
                }
            }
        }
    }

    public static void main(String[] args) {

        SparkplugRaspberryPiExample example = new SparkplugRaspberryPiExample(args);
        example.run();
    }

    public void run() {
        try {
            // Create the Raspberry Pi Pibrella board listeners
            if (this.isAPi) {
                createPibrellaListeners();
            }
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

                        if(debug){
                            System.out.println("-----------RTU VALUES-----------");
                        }

                        for (Entry<String, TagValue> t : rtu.values.entrySet()) {
                            if(debug){
                                System.out.println("key " + t.getKey() + "   \tvalue " + t.getValue().getValue());
                            }

                            if (t.getValue().updateValue()) {
                                outboundPayload.addMetric(t.getKey(), t.getValue().getValue());
                            }
                        }

                        if(debug){
                            System.out.println("----------------------------------");
                            System.out.println("Meter 1's data: ");
                            Meter m1 = rtu.meters.get("meter_1");
                            for(String key : m1.keySet()){
                                System.out.println("- key: " + key + "\t val: " + m1.get(key).getValue());
                            }
                        }

                        // Publish the message
                        executor.execute(new Publisher("spAv1.0/" + groupId + "/NDATA/" + edgeNode, outboundPayload));

                        outboundPayload = new KuraPayload();
                        outboundPayload.setTimestamp(new Date());
                        outboundPayload = addSeqNum(outboundPayload);

                        for (Meter m : rtu.meters.values()) {
                            m.updateMeter(outboundPayload, false);
                        }
                        executor.execute(new Publisher("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/meters", outboundPayload));
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

            options.setServerURIs(servers);

            if (!(boolean) settings.get("anonymous")) {
                // MQTT Client Username
                options.setUserName(username);
                // MQTT Client Password
                options.setPassword(password.toCharArray());
            }

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

            client = new MqttClient(servers[0], clientId);

            //
            // Using the parameters set above, try to connect to the define MQTT
            // server now.
            //
            //System.out.println("Trying to establish an MQTT Session to the MQTT Server @ :" + serverUrl);
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
            if ((boolean) settings.get("updatable")) {
                client.subscribe("kymera/#", 0);
            }
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

                String ipAddress = "unknown";
                for (NetworkInterface netint : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                    if (adapter.equals(netint.getName())) {
                        for (InetAddress inetAddress : Collections.list(netint.getInetAddresses())) {
                            if (inetAddress instanceof Inet4Address) {
                                ipAddress = inetAddress.getHostAddress();
                            }
                        }
                    }
                }

                payload.addMetric("Properties/Ip_adr", ipAddress);

                // Build a GPS Position object for the payload. Note that the
                // Position object is optional.
                // The Position parameters will be populated in "Position"
                // folder in Ignition
                // if present.
                KuraPosition position = new KuraPosition();
                position.setAltitude(319);
                position.setHeading(0);
                position.setLatitude((double) settings.get("latitude"));
                position.setLongitude((double) settings.get("longitude"));
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

                for (Entry<String, TagValue> t : rtu.values.entrySet()) {
                    payload.addMetric(t.getKey(), t.getValue().getValue());
                }

                executor.execute(new Publisher("spAv1.0/" + groupId + "/NBIRTH/" + edgeNode, payload));

                //
                // Create the Device BIRTH Certificate now. The tags defined
                // here will appear in a
                // folder hierarchy under the associated Device.
                //

                if (isAPi) {
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
                payload = new KuraPayload();
                payload.setTimestamp(new Date());
                payload = addSeqNum(payload);

                for (Meter m : rtu.meters.values()) {
                    m.updateMeter(payload, true);
                }

                executor.execute(new Publisher("spAv1.0/" + groupId + "/DBIRTH/" + edgeNode + "/meters", payload));
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
     */
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        //System.out.println("Message Arrived on topic " + topic);

        // Initialize the outbound payload if required.
        KuraPayload outboundPayload = new KuraPayload();
        outboundPayload.setTimestamp(new Date());
        outboundPayload = addSeqNum(outboundPayload);

        boolean sendPayload = false;
        boolean reboot = false;
        String[] splitTopic = topic.split("/");
        System.out.println(topic);
        if (splitTopic[0].equals("spAv1.0") && splitTopic[1].equals(groupId) && splitTopic[2].equals("NCMD")
                && splitTopic[3].equals(edgeNode)) {

            CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
            KuraPayload inboundPayload = decoder.buildFromByteArray();

            Iterator<Entry<String, Object>> metrics = inboundPayload.metrics().entrySet().iterator();
            while (metrics.hasNext()) {
                Entry<String, Object> entry = metrics.next();
                //System.out.println("Metric: " + entry.getKey() + " :: " + entry.getValue());

                if ("Node Control/Rebirth".equals(entry.getKey())) {
                    if ((boolean) entry.getValue()) {
                        publishBirth();
                    }
                } else if ("Node Control/Reboot".equals(entry.getKey())) {
                    if ((boolean) entry.getValue() && isAPi) {
                        System.out.println("Received a Reboot command.");
                        outboundPayload.addMetric("Node Control/Reboot", false);
                        sendPayload = true;
                        reboot = true;
                    }
                } else if ("Node Control/Next Server".equals(entry.getKey())) {
                    if ((boolean) entry.getValue()) {

                        System.out.println("Received a Next Server command.");
                    }
                } else if ("Node Control/Scan Rate ms".equals(entry.getKey())) {
                    scanRateMs = (Integer) inboundPayload.getMetric("Node Control/Scan Rate ms");
                    if (scanRateMs < 100) {
                        // Limit Scan Rate to a minimum of 100ms
                        scanRateMs = 100;
                    }
                    outboundPayload.addMetric("Node Control/Scan Rate ms", scanRateMs);
                    sendPayload = true;
                } else {
                    TagValue v = rtu.values.get(entry.getKey());
                    if (v != null) {
                        v.setValue(entry.getValue(), false);
                        outboundPayload.addMetric(entry.getKey(), v.getValue());
                        sendPayload = true;
                    }
                }

                synchronized (lock) {
                    if (sendPayload) {
                        executor.execute(new Publisher("spAv1.0/" + groupId + "/NDATA/" + edgeNode, outboundPayload));
                    }
                }

                if (reboot) {
                    if (isAPi) {
                        Runtime.getRuntime().exec("reboot");
                    }
                }
            }
        } else if (splitTopic[0].equals("spAv1.0") && splitTopic[1].equals(groupId) && splitTopic[2].equals("DCMD")
                && splitTopic[3].equals(edgeNode) && splitTopic[4].equals(deviceId)) {
            synchronized (lock) {
                System.out.println("Command received for device: " + splitTopic[4] + " on topic: " + topic);

                // Get the incoming metric key and value
                CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
                KuraPayload inboundPayload = decoder.buildFromByteArray();

                Iterator<Entry<String, Object>> metrics = inboundPayload.metrics().entrySet().iterator();
                while (metrics.hasNext()) {
                    Entry<String, Object> entry = metrics.next();
                    //System.out.println("Metric: " + entry.getKey() + " :: " + entry.getValue());
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
        } else if (splitTopic[0].equals("spAv1.0") && splitTopic[1].equals(groupId) && splitTopic[2].equals("DCMD")
                && splitTopic[3].equals(edgeNode) && splitTopic[4].equals("meters")) {
            synchronized (lock) {
                System.out.println("Command received for device: " + splitTopic[4] + " on topic: " + topic);

                // Get the incoming metric key and value
                CloudPayloadProtoBufDecoderImpl decoder = new CloudPayloadProtoBufDecoderImpl(message.getPayload());
                KuraPayload inboundPayload = decoder.buildFromByteArray();

                Iterator<Entry<String, Object>> metrics = inboundPayload.metrics().entrySet().iterator();
                while (metrics.hasNext()) {
                    Entry<String, Object> entry = metrics.next();
                    System.out.println("Metric: " + entry.getKey() + " :: " + entry.getValue());
                    String[] splitMetric = entry.getKey().split("/");
                    Meter m = rtu.meters.get(splitMetric[0]);
                    if (m != null) {

                        String metric = String.join("/", Arrays.copyOfRange(splitMetric, 0, splitMetric.length));
                        System.out.println(metric);
                        TagValue v = m.get(metric);
                        if (v != null) {
                            v.setValue(entry.getValue(), false);
                            outboundPayload.addMetric(entry.getKey(), v.getValue());
                        }
                    }
                }

                executor.execute(
                        new Publisher("spAv1.0/" + groupId + "/DDATA/" + edgeNode + "/meters", outboundPayload));
            }

        } else if (splitTopic[0].equals("kymera") && splitTopic[1].equals("upgrade")) {
            byte[] bytes = message.getPayload();
            if (bytes.length > 0) {
                //byte[] bytes = Base64.getDecoder().decode((String) entry.getValue());
                FileOutputStream stream = new FileOutputStream("/home/pi/sparkplug-rtu.jar");
                try {
                    stream.write(bytes);
                } finally {
                    stream.close();
                }
                Runtime.getRuntime().exec("reboot");
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

    private void readConfig() {

        try {

            ObjectMapper mapper = new ObjectMapper();

            // read JSON from a file
            Map<String, Object> map = mapper.readValue(
                    new File(settingsFile),
                    new TypeReference<Map<String, Object>>() {
                    });

            for (Entry<String, Object> e : map.entrySet()) {
                settings.put(e.getKey(), e.getValue());
            }


        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeConfig() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(new File(settingsFile), settings);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setupModbusSlave() {
        ModbusTcpSlaveConfig mtsc = new ModbusTcpSlaveConfig.Builder().build();
        slave = new ModbusTcpSlave(mtsc);

        slave.setRequestHandler(new ServiceRequestHandler() {
/*            @Override
            public void onReadCoils(ServiceRequest<ReadCoilsRequest, ReadCoilsResponse> service) {
                System.out.println(String.format("unit: %d coils: %d count: %d", service.getUnitId(), service.getRequest().getAddress(), service.getRequest().getQuantity()));
                ReadCoilsRequest request = service.getRequest();
                ByteBuf coils = PooledByteBufAllocator.DEFAULT.buffer(1);
                coils.writeBoolean(false);
                coils.writeBoolean(true);
                coils.writeBoolean(false);
                coils.writeBoolean(true);
                coils.writeBoolean(true);
                coils.writeBoolean(true);
                coils.writeBoolean(false);
                service.sendResponse(new ReadCoilsResponse(coils));

                ReferenceCountUtil.release(request);
            }
*/
            @Override
            public void onReadHoldingRegisters(ServiceRequest<ReadHoldingRegistersRequest, ReadHoldingRegistersResponse> service) {
                System.out.println(String.format("unit: %d registerIndex: %d count: %d", service.getUnitId(), service.getRequest().getAddress(), service.getRequest().getQuantity()));
                ReadHoldingRegistersRequest request = service.getRequest();
                HashMap map = modbusRegisters.get(service.getUnitId());
                ByteBuf registers = PooledByteBufAllocator.DEFAULT.buffer(request.getQuantity());

                int registerIndex = request.getAddress();

                if (map != null) {
                    try {

                        while (registerIndex < request.getAddress() + request.getQuantity()) {

                            TagValue tv = (TagValue) map.get(registerIndex);
                            if (tv != null) {
                                Object value = tv.getValue();
                                if (value instanceof Float) {
                                    registers.writeFloat((Float) value);
                                   // System.out.println(String.format("Reference found %d,%d", registerIndex,2));
                                    registerIndex += 2;
                                } else if (value instanceof Integer) {
                                    registers.writeInt((Integer) value);
                                    //System.out.println(String.format("Reference found %d,%d", registerIndex,2));
                                    registerIndex += 2;
                                } else if (value instanceof Double) {
                                    registers.writeDouble((Double) value);
                                    //System.out.println(String.format("Reference found %d,%d", registerIndex,4));
                                    registerIndex += 4;
                                } else if (value instanceof Long) {
                                    registers.writeLong((Long) value);
                                    //System.out.println(String.format("Reference found %d,%d", registerIndex,4));
                                    registerIndex += 4;
                                }

                            } else {
                                registers.writeChar(0);
                                registerIndex += 1;
                            }
                        }

                    } catch (Exception e) {
                    }
                } else {
                    System.out.println(String.format("Map Not Found: %d", service.getUnitId()));
                }
                service.sendResponse(new ReadHoldingRegistersResponse(registers));

                ReferenceCountUtil.release(request);
            }
        });
        try {
            slave.bind(bindUrl, 502).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
