package com.cirruslink.example;

import com.cirruslink.example.impl.DateValue;
import com.cirruslink.example.impl.FloatValue;
import com.cirruslink.example.impl.StringValue;
import com.cirruslink.example.model.TagValue;
import com.digitalpetri.modbus.ModbusPdu;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class RTU {
    private final String tagPath;
    Map<String,Meter> meters = new HashMap<>();
    Map<String,TagValue> values = new HashMap<>();

    public RTU(String rtu, int count){

        this.tagPath = rtu;
        for (short i = 1; i < count+1; i++) {
            Meter meter = new Meter(String.format("meter_%d", i));
            meters.put(String.format("meter_%d", i),meter);
            SparkplugRaspberryPiExample.modbusRegisters.put(i,meter.registers);
            SparkplugRaspberryPiExample.modbusCoils.put(i,meter.registers);

        }

        values.put("rtu/stats/volts",new FloatValue(0.0f,12.0f,0.1f,0.3f));
        values.put("rtu/stats/temp",new FloatValue(0.0f,100.0f,0.1f,0.3f));
        values.put("rtu/date",new DateValue());
        values.put("rtu/metadata/Meter ID",new StringValue(tagPath));
    }


}
