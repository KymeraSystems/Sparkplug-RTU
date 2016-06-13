package com.cirruslink.example;

import com.cirruslink.example.impl.DateValue;
import com.cirruslink.example.impl.FloatValue;
import com.cirruslink.example.model.TagValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class RTU {
    private final String tagPath;
    Map<String,Meter> meters = new HashMap();
    Map<String,TagValue> values = new HashMap<>();

    public RTU(String rtu){
        this.tagPath = rtu;
        for (int i = 0; i < 5; i++) {
            meters.put(String.format("meter_%d", i),new Meter(String.format("meter_%d", i)));
        }

        values.put(String.format("%s","volts"),new FloatValue(0.0f,12.0f,0.1f,0.3f));
        values.put(String.format("%s","temp"),new FloatValue(0.0f,100.0f,0.1f,0.3f));
        values.put(String.format("%s","date"),new DateValue());
    }


}
