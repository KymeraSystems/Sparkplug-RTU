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

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <mosquitto.h>
#include "kurapayload.pb-c.h"

#include <time.h>
#include <sys/time.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

int32_t seqNum = 0;
int32_t bdSeqNum = 0;

int64_t get_current_timestamp();
void publisher(struct mosquitto *mosq, char *topic, Kuradatatypes__KuraPayload payload);
void my_message_callback(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *message);
void my_connect_callback(struct mosquitto *mosq, void *userdata, int result);
void my_subscribe_callback(struct mosquitto *mosq, void *userdata, int mid, int qos_count, const int *granted_qos);
void my_log_callback(struct mosquitto *mosq, void *userdata, int level, const char *str);
Kuradatatypes__KuraPayload__KuraMetric *getDoubleMetric(char *name, double value);
Kuradatatypes__KuraPayload__KuraMetric *getFloatMetric(char *name, float value);
Kuradatatypes__KuraPayload__KuraMetric *getLongMetric(char *name, long value);
Kuradatatypes__KuraPayload__KuraMetric *getIntMetric(char *name, int value);
Kuradatatypes__KuraPayload__KuraMetric *getBooleanMetric(char *name, protobuf_c_boolean value);
Kuradatatypes__KuraPayload__KuraMetric *getStringMetric(char *name, char *value);
Kuradatatypes__KuraPayload__KuraMetric *getBytesMetric(char *name, ProtobufCBinaryData value);
Kuradatatypes__KuraPayload__KuraMetric *getNextSeqMetric();
Kuradatatypes__KuraPayload getNextPayload(bool firstData);
void freePayload(Kuradatatypes__KuraPayload *payload);

int64_t get_current_timestamp() {
	// Set the timestamp
	struct timespec ts;
	#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
		clock_serv_t cclock;
		mach_timespec_t mts;
		host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
		clock_get_time(cclock, &mts);
		mach_port_deallocate(mach_task_self(), cclock);
		ts.tv_sec = mts.tv_sec;
		ts.tv_nsec = mts.tv_nsec;
	#else
		clock_gettime(CLOCK_REALTIME, &ts);
	#endif
	return ts.tv_sec * INT64_C(1000) + ts.tv_nsec / 1000000;
}

void publisher(struct mosquitto *mosq, char *topic, Kuradatatypes__KuraPayload payload) {
	void *buf;
	unsigned len;
	len = kuradatatypes__kura_payload__get_packed_size(&payload);
	buf = malloc(len);
	kuradatatypes__kura_payload__pack(&payload, buf);

	// publish the data
	mosquitto_publish(mosq, NULL, topic, len, buf, 0, false);
	free(buf);
	freePayload(&payload);
}

void my_message_callback(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *message) {
	if(message->payloadlen) {
		printf("%s :: %d\n", message->topic, message->payloadlen);

		// Parse the payload
		Kuradatatypes__KuraPayload *inboundPayload;
		inboundPayload = kuradatatypes__kura_payload__unpack(NULL, message->payloadlen, message->payload);

		// Prepare an action and response
		Kuradatatypes__KuraPayload outboundPayload = KURADATATYPES__KURA_PAYLOAD__INIT;
		outboundPayload.has_timestamp = true;
		outboundPayload.timestamp = get_current_timestamp();

		printf("Metric: %s\n", inboundPayload->metric[0]->name);
		if(strcmp(inboundPayload->metric[0]->name, "output0") == 0) {
			Kuradatatypes__KuraPayload__KuraMetric **metrics;
			metrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 3);
		        metrics[0] = getNextSeqMetric();
			metrics[1] = getBooleanMetric("input0", inboundPayload->metric[0]->bool_value);
			metrics[2] = getBooleanMetric("output0", inboundPayload->metric[0]->bool_value);
			outboundPayload.n_metric = 3;
		        outboundPayload.metric = metrics;
		} else if(strcmp(inboundPayload->metric[0]->name, "output1") == 0) {
			Kuradatatypes__KuraPayload__KuraMetric **metrics;
			metrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 3);
		        metrics[0] = getNextSeqMetric();
			metrics[1] = getIntMetric("input1", inboundPayload->metric[0]->int_value);
			metrics[2] = getIntMetric("output1", inboundPayload->metric[0]->int_value);
			outboundPayload.n_metric = 3;
		        outboundPayload.metric = metrics;
		} else if(strcmp(inboundPayload->metric[0]->name, "output2") == 0) {
			Kuradatatypes__KuraPayload__KuraMetric **metrics;
			metrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 3);
		        metrics[0] = getNextSeqMetric();
			metrics[1] = getFloatMetric("input2", inboundPayload->metric[0]->float_value);
			metrics[2] = getFloatMetric("output2", inboundPayload->metric[0]->float_value);
			outboundPayload.n_metric = 3;
		        outboundPayload.metric = metrics;
		} else {
			// Unknown type
			return;
		}

		//printf("Device input size %zu\n", kuradatatypes__kura_payload__get_packed_size(&outboundPayload));
		printf("Seq Num (command): %lld\n", outboundPayload.metric[0]->long_value);
		publisher(mosq, "spv1.0/Sparkplug Devices/DDATA/C Edge Node/Emulated Device", outboundPayload);
	} else {
		printf("%s (null)\n", message->topic);
	}
	fflush(stdout);
}

void my_connect_callback(struct mosquitto *mosq, void *userdata, int result) {
	if(!result) {
		// Subscribe to commands
		mosquitto_subscribe(mosq, NULL, "spv1.0/Sparkplug Devices/NCMD/C Edge Node/#", 0);
		mosquitto_subscribe(mosq, NULL, "spv1.0/Sparkplug Devices/DCMD/C Edge Node/#", 0);
	} else {
		fprintf(stderr, "Connect failed\n");
	}
}

void my_subscribe_callback(struct mosquitto *mosq, void *userdata, int mid, int qos_count, const int *granted_qos) {
	int i;

	printf("Subscribed (mid: %d): %d", mid, granted_qos[0]);
	for(i=1; i<qos_count; i++) {
		printf(", %d", granted_qos[i]);
	}
	printf("\n");
}

void my_log_callback(struct mosquitto *mosq, void *userdata, int level, const char *str) {
	/* Pring all log messages regardless of level. */
	printf("%s\n", str);
}

Kuradatatypes__KuraPayload__KuraMetric *getDoubleMetric(char *name, double value) {
	Kuradatatypes__KuraPayload__KuraMetric *metric;
	metric = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric));
	kuradatatypes__kura_payload__kura_metric__init(metric);
	metric->name = name;
	metric->type = KURADATATYPES__KURA_PAYLOAD__KURA_METRIC__VALUE_TYPE__DOUBLE;
	metric->has_double_value = true;
	metric->double_value = value;
	return metric;
}

Kuradatatypes__KuraPayload__KuraMetric *getFloatMetric(char *name, float value) {
	Kuradatatypes__KuraPayload__KuraMetric *metric;
	metric = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric));
	kuradatatypes__kura_payload__kura_metric__init(metric);
	metric->name = name;
	metric->type = KURADATATYPES__KURA_PAYLOAD__KURA_METRIC__VALUE_TYPE__FLOAT;
	metric->has_float_value = true;
	metric->float_value = value;
	return metric;
}

Kuradatatypes__KuraPayload__KuraMetric *getLongMetric(char *name, long value) {
	Kuradatatypes__KuraPayload__KuraMetric *metric;
	metric = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric));
	kuradatatypes__kura_payload__kura_metric__init(metric);
	metric->name = name;
	metric->type = KURADATATYPES__KURA_PAYLOAD__KURA_METRIC__VALUE_TYPE__INT64;
	metric->has_long_value = true;
	metric->long_value = value;
	return metric;
}

Kuradatatypes__KuraPayload__KuraMetric *getIntMetric(char *name, int value) {
	Kuradatatypes__KuraPayload__KuraMetric *metric;
	metric = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric));
	kuradatatypes__kura_payload__kura_metric__init(metric);
	metric->name = name;
	metric->type = KURADATATYPES__KURA_PAYLOAD__KURA_METRIC__VALUE_TYPE__INT32;
	metric->has_int_value = true;
	metric->int_value = value;
	return metric;
}

Kuradatatypes__KuraPayload__KuraMetric *getBooleanMetric(char *name, protobuf_c_boolean value) {
	Kuradatatypes__KuraPayload__KuraMetric *metric;
	metric = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric));
	kuradatatypes__kura_payload__kura_metric__init(metric);
	metric->name = name;
	metric->type = KURADATATYPES__KURA_PAYLOAD__KURA_METRIC__VALUE_TYPE__BOOL;
	metric->has_bool_value = true;
	metric->bool_value = value;
	return metric;
}

Kuradatatypes__KuraPayload__KuraMetric *getStringMetric(char *name, char *value) {
	Kuradatatypes__KuraPayload__KuraMetric *metric;
	metric = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric));
	kuradatatypes__kura_payload__kura_metric__init(metric);
	metric->name = name;
	metric->type = KURADATATYPES__KURA_PAYLOAD__KURA_METRIC__VALUE_TYPE__STRING;
	metric->string_value = value;
	return metric;
}

Kuradatatypes__KuraPayload__KuraMetric *getBytesMetric(char *name, ProtobufCBinaryData value) {
	Kuradatatypes__KuraPayload__KuraMetric *metric;
	metric = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric));
	kuradatatypes__kura_payload__kura_metric__init(metric);
	metric->name = name;
	metric->type = KURADATATYPES__KURA_PAYLOAD__KURA_METRIC__VALUE_TYPE__BYTES;
	metric->has_bytes_value = true;
	metric->bytes_value = value;
	return metric;
}

Kuradatatypes__KuraPayload__KuraMetric *getNextSeqMetric() {
	if(seqNum == 256) {
		seqNum = 0;
	}
	int tmpNum = seqNum;
	seqNum++;
	return getIntMetric("seq", tmpNum);
}

Kuradatatypes__KuraPayload__KuraMetric *getNextBdSeqMetric() {
	if(bdSeqNum == 256) {
		bdSeqNum = 0;
	}
	int tmpNum = bdSeqNum;
	bdSeqNum++;
	return getIntMetric("bdSeq", tmpNum);
}

Kuradatatypes__KuraPayload getNextPayload(bool birth) {

	if(birth) {
		Kuradatatypes__KuraPayload payload = KURADATATYPES__KURA_PAYLOAD__INIT;
	        Kuradatatypes__KuraPayload__KuraMetric **metrics;

	        metrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 11);
		metrics[0] = getBooleanMetric("my_bool", rand()%2);
		metrics[1] = getDoubleMetric("my_double", ((double)rand()/(double)100));
		metrics[2] = getFloatMetric("my_float", ((float)rand()/(float)100));
		metrics[3] = getIntMetric("my_int", rand());
		metrics[4] = getLongMetric("my_long", (long)rand());
		metrics[5] = getBooleanMetric("input0", true);
		metrics[6] = getIntMetric("input1", 0);
		metrics[7] = getFloatMetric("input2", 1.23);
		metrics[8] = getBooleanMetric("output0", true);
		metrics[9] = getIntMetric("output1", 0);
		metrics[10] = getFloatMetric("output2", 1.23);
	        payload.n_metric = 11;
	        payload.metric = metrics;

		// device parameters
		Kuradatatypes__KuraPayload deviceParameterPayload = KURADATATYPES__KURA_PAYLOAD__INIT;
	        Kuradatatypes__KuraPayload__KuraMetric **deviceParameterMetrics;
	        deviceParameterMetrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 2);
		deviceParameterMetrics[0] = getStringMetric("hw_version", "3.2.0");
		deviceParameterMetrics[1] = getStringMetric("sw_version", "1.0.0");

		Kuradatatypes__KuraPayload totalPayload = KURADATATYPES__KURA_PAYLOAD__INIT;

		// Set the timestamp
		totalPayload.has_timestamp = true;
		totalPayload.timestamp = get_current_timestamp();

	        Kuradatatypes__KuraPayload__KuraMetric **totalMetrics;
	        totalMetrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 3);
	        totalMetrics[0] = getNextSeqMetric();

		// Get the pv_map bytes
		void *buf;
		unsigned len;
		len = kuradatatypes__kura_payload__get_packed_size(&payload);
		buf = malloc(len);
		kuradatatypes__kura_payload__pack(&payload, buf);
		struct ProtobufCBinaryData data;
		data.len = len;
		data.data = buf;
		totalMetrics[1] = getBytesMetric("pv_map", data);

		// Get the device_parameter bytes
		void *deviceParameterBuf;
		unsigned deviceParameterLen;
		deviceParameterLen = kuradatatypes__kura_payload__get_packed_size(&deviceParameterPayload);
		deviceParameterBuf = malloc(deviceParameterLen);
		kuradatatypes__kura_payload__pack(&deviceParameterPayload, deviceParameterBuf);
		struct ProtobufCBinaryData deviceParameterData;
		deviceParameterData.len = deviceParameterLen;
		deviceParameterData.data = deviceParameterBuf;
		totalMetrics[2] = getBytesMetric("device_parameters", deviceParameterData);

		totalPayload.n_metric = 3;
		totalPayload.metric = totalMetrics;
		return totalPayload;

		/* // TODO - need to handle cleanup
		free(buf);
		freePayload(&payload);
		free(deviceParameterBuf);
		freePayload(&deviceParameterPayload);
		*/
	} else {
		Kuradatatypes__KuraPayload payload = KURADATATYPES__KURA_PAYLOAD__INIT;

		// Set the timestamp
		payload.has_timestamp = true;
		payload.timestamp = get_current_timestamp();

	        Kuradatatypes__KuraPayload__KuraMetric **metrics;

		metrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 6);
	        metrics[0] = getNextSeqMetric();
		metrics[1] = getBooleanMetric("my_bool", rand()%2);
		metrics[2] = getDoubleMetric("my_double", ((double)rand()/(double)100));
		metrics[3] = getFloatMetric("my_float", ((float)rand()/(float)100));
		metrics[4] = getIntMetric("my_int", rand());
		metrics[5] = getLongMetric("my_long", (long)rand());
		payload.n_metric = 6;

		// Add the metric to the payload and set the number of metrics
	        payload.metric = metrics;
		return payload;
	}
}

void freePayload(Kuradatatypes__KuraPayload *payload) {
	// Get the metrics and free them
	for(int i=0; i<payload->n_metric; i++) {
		free(payload->metric[i]);
	}
	free(payload->metric);
}

void publishBirth(struct mosquitto *mosq) {
	// create a birth cert
	Kuradatatypes__KuraPayload birthPayload = KURADATATYPES__KURA_PAYLOAD__INIT;

	// Set the timestamp
	birthPayload.has_timestamp = true;
	birthPayload.timestamp = get_current_timestamp();

	// Set the Position
	Kuradatatypes__KuraPayload__KuraPosition position = KURADATATYPES__KURA_PAYLOAD__KURA_POSITION__INIT;
	position.has_altitude = true;
	position.altitude = 319;
	position.has_heading = true;
	position.heading = 0;
	position.latitude = 38.83667239;
	position.longitude = -94.67176706;
	position.has_precision = true;
	position.precision = 2.0;
	position.has_satellites = true;
	position.satellites = 8;
	position.has_speed = true;
	position.speed = 0;
	position.has_status = true;
	position.status = 3;
	position.has_timestamp = true;
	position.timestamp = birthPayload.timestamp;
	birthPayload.position = &position;

	// Add the sequence metric to the payload and set the number of metrics
	Kuradatatypes__KuraPayload__KuraMetric **metrics;
	metrics = malloc(sizeof(Kuradatatypes__KuraPayload__KuraMetric*) * 1);
	seqNum = 0;
	metrics[0] = getNextSeqMetric();
	printf("Seq Num (birth): %lld\n", metrics[0]->long_value);
	birthPayload.metric = metrics;
	birthPayload.n_metric = 1;

	//printf("birth size %zu\n", kuradatatypes__kura_payload__get_packed_size(&birthPayload));
	publisher(mosq, "spv1.0/Sparkplug Devices/NBIRTH/C Edge Node", birthPayload);

	// The first payload includes all inputs and outputs that will be driven asynchronously after the first message
	Kuradatatypes__KuraPayload deviceBirthPayload;
	deviceBirthPayload = getNextPayload(true);

	//printf("Device birth size %zu\n", kuradatatypes__kura_payload__get_packed_size(&deviceBirthPayload));
	publisher(mosq, "spv1.0/Sparkplug Devices/DBIRTH/C Edge Node/Emulated Device", deviceBirthPayload);
}

int main(int argc, char *argv[])
{
	char *host = "localhost";
	int port = 1883;
	int keepalive = 60;
	bool clean_session = true;
	struct mosquitto *mosq = NULL;

	srand(time(NULL));

	mosquitto_lib_init();
	mosq = mosquitto_new(NULL, clean_session, NULL);
	if(!mosq){
		fprintf(stderr, "Error: Out of memory.\n");
		return 1;
	}
	mosquitto_log_callback_set(mosq, my_log_callback);
	mosquitto_connect_callback_set(mosq, my_connect_callback);
	mosquitto_message_callback_set(mosq, my_message_callback);
	mosquitto_subscribe_callback_set(mosq, my_subscribe_callback);
	mosquitto_username_pw_set(mosq,"admin","changeme");
	mosquitto_will_set(mosq, "spv1.0/Sparkplug Devices/NDEATH/C Edge Node", 0, NULL, 0, false);

	if(mosquitto_connect(mosq, host, port, keepalive)){
		fprintf(stderr, "Unable to connect.\n");
		return 1;
	}

	publishBirth(mosq);

	// Loop and publish more messages
	for(int i=0; i<100; i++) {
		Kuradatatypes__KuraPayload payload;
		payload = getNextPayload(false);

		//printf("data size %zu\n", kuradatatypes__kura_payload__get_packed_size(&payload));
		printf("Seq Num (data): %lld\n", payload.metric[0]->long_value);
		publisher(mosq, "spv1.0/Sparkplug Devices/DDATA/C Edge Node/Emulated Device", payload);

		for(int j=0; j<50; j++) {
			usleep(100000);
			mosquitto_loop(mosq, 0, 1);
		}
	}

	mosquitto_destroy(mosq);
	mosquitto_lib_cleanup();
	return 0;
}
