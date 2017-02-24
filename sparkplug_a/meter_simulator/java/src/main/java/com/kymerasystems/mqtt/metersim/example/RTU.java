package com.kymerasystems.mqtt.metersim.example;

import com.kymerasystems.mqtt.metersim.example.impl.DateValue;
import com.kymerasystems.mqtt.metersim.example.impl.FloatValue;
import com.kymerasystems.mqtt.metersim.example.impl.StringValue;
import com.kymerasystems.mqtt.metersim.example.model.TagValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class RTU {
    private final String tagPath;
    Map<String,Meter> meters = new HashMap<>();
    Map<String,TagValue> values = new HashMap<>();

    public RTU(String rtu, int meterCount){

        this.tagPath = rtu;
        for (short i = 1; i < meterCount+1; i++) {
            Meter meter = new Meter(String.format("meter_%d", i));
            meters.put(String.format("meter_%d", i),meter);
            MeterMain.modbusRegisters.put(i,meter.registers);
            MeterMain.modbusCoils.put(i,meter.registers);
        }

        values.put("rtu/stats/volts",new FloatValue(0.0f,12.0f,0.1f,0.3f));
        values.put("rtu/stats/temp",new FloatValue(0.0f,100.0f,0.1f,0.3f));
        values.put("rtu/date",new DateValue());
        values.put("rtu/metadata/Meter ID",new StringValue(tagPath));
    }


}
