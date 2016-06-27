package com.cirruslink.example;

import com.cirruslink.example.impl.*;
import com.cirruslink.example.model.TagValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.kura.message.KuraPayload;

import java.util.*;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class Meter extends HashMap<String, TagValue> {
    String tagPath;

    public Meter(String tagPath) {
        this.tagPath = tagPath;
        createAnalog(0.0f, 2000.0f, 0.1f, 1.0f, "/meter/sp");
        createAnalog(0.0f, 200.0f, 0.1f, 0.2f, "/meter/dp");
        createAnalog(0.0f, 100.0f, 0.1f, 0.4f, "/meter/temp");
        createAnalog(0.0f, 500.0f, 0.1f, 0.5f, "/meter/flow rate");
        createAnalog(0.0f, 2000.0f, 0.1f, 5.0f, "/casing pressure");
        this.put(tagPath + "/esd valve", new BooleanValue());
        this.put(tagPath + "/meter/today/volume", new TotalizerValue(1000l, 0.01f));
        this.put(tagPath + "/meter/yday/volume", new MemoryFloatValue());
        this.put(tagPath + "/meter/records/hourly", new StringValue(""));
        this.put(tagPath + "/meter/records/daily", new StringValue(""));
        this.put(tagPath + "/meter/analysis/co2", new MemoryFloatValue(3.11));
        this.put(tagPath + "/meter/analysis/n2", new MemoryFloatValue(2.2));
        this.put(tagPath + "/meter/analysis/c1", new MemoryFloatValue(93.34));
        this.put(tagPath + "/meter/analysis/c2", new MemoryFloatValue(0.55));
        this.put(tagPath + "/meter/analysis/c3", new MemoryFloatValue(0.2));
        this.put(tagPath + "/meter/analysis/ic4", new MemoryFloatValue(0.07));
        this.put(tagPath + "/meter/analysis/nc4", new MemoryFloatValue(0.07));
        this.put(tagPath + "/meter/analysis/ic5", new MemoryFloatValue(0.03));
        this.put(tagPath + "/meter/analysis/nc5", new MemoryFloatValue(0.05));
        this.put(tagPath + "/meter/analysis/c6", new MemoryFloatValue(0.07));
        this.put(tagPath + "/meter/analysis/c7", new MemoryFloatValue(0.07));
        this.put(tagPath + "/meter/analysis/c8", new MemoryFloatValue(0.08));
        this.put(tagPath + "/meter/analysis/c9", new MemoryFloatValue(0.03));
        this.put(tagPath + "/meter/analysis/c10", new MemoryFloatValue(0.01));
        this.put(tagPath + "/meter/analysis/h2o", new MemoryFloatValue(0.0));
        this.put(tagPath + "/meter/analysis/h2s", new MemoryFloatValue(0.0));
        this.put(tagPath + "/meter/analysis/h2", new MemoryFloatValue(0.03));
        this.put(tagPath + "/meter/analysis/co", new MemoryFloatValue(0.0));
        this.put(tagPath + "/meter/analysis/he", new MemoryFloatValue(0.09));
        this.put(tagPath + "/meter/analysis/o2", new MemoryFloatValue(0.0));
        this.put(tagPath + "/meter/config/pipe diameter", new MemoryFloatValue(0.0));
        this.put(tagPath + "/meter/config/orifice diameter", new MemoryFloatValue(0.0));

    }

    public void updateMeter(KuraPayload payload, boolean init) {

        Calendar c = Calendar.getInstance();

        for (Entry<String, TagValue> e : this.entrySet()) {
            if (init || e.getValue().updateValue()) {
                payload.addMetric(e.getKey(), e.getValue().getValue());
            }
        }


        if (/*c.get(Calendar.MINUTE) == 0 && */c.get(Calendar.SECOND) == 0) {
            createHourlyJSONRecord(payload);
        }

        if (/*c.get(Calendar.HOUR_OF_DAY) == 0 && c.get(Calendar.MINUTE) == 0 &&*/ c.get(Calendar.SECOND) == 0) {
            createDailyRecord(payload);
        }
    }

    public void createAnalog(Float lowValue, Float highValue, Float variance, Float deadBand, String basePath) {
        // System.out.println(String.format("Tag Path:%s || Base Path:%s", tagPath, basePath));
        FloatValue value = new FloatValue(lowValue, highValue, variance, deadBand);
        this.put(tagPath + basePath + "/value", value);


        MemoryBooleanValue en = new MemoryBooleanValue(true);
        MemoryFloatValue sp = new MemoryFloatValue(highValue * 0.05);
        this.put(tagPath + basePath + "/alm/ll/en", en);
        this.put(tagPath + basePath + "/alm/ll/sp", sp);
        this.put(tagPath + basePath + "/alm/ll/alarm", new AnalogAlarmValue(en, value, sp, AnalogAlarmValue.AlarmType.LOW));

        en = new MemoryBooleanValue(true);
        sp = new MemoryFloatValue(highValue * 0.1);
        this.put(tagPath + basePath + "/alm/l/en", en);
        this.put(tagPath + basePath + "/alm/l/sp", sp);
        this.put(tagPath + basePath + "/alm/l/alarm", new AnalogAlarmValue(en, value, sp, AnalogAlarmValue.AlarmType.LOW));

        en = new MemoryBooleanValue(true);
        sp = new MemoryFloatValue(highValue * 0.90);
        this.put(tagPath + basePath + "/alm/h/en", en);
        this.put(tagPath + basePath + "/alm/h/sp", sp);
        this.put(tagPath + basePath + "/alm/h/alarm", new AnalogAlarmValue(en, value, sp, AnalogAlarmValue.AlarmType.HIGH));

        en = new MemoryBooleanValue(true);
        sp = new MemoryFloatValue(highValue * 0.95);
        this.put(tagPath + basePath + "/alm/hh/en", en);
        this.put(tagPath + basePath + "/alm/hh/sp", sp);
        this.put(tagPath + basePath + "/alm/hh/alarm", new AnalogAlarmValue(en, value, sp, AnalogAlarmValue.AlarmType.HIGH));
    }


    public void createHourlyJSONRecord(KuraPayload payload) {
        try {
            HashMap<String, Object> map = new HashMap<>();
            map.put("t_stamp", new Date());
            map.put("id", tagPath);
            map.put("value", get(String.format("%s/meter/%s", tagPath, "today/volume")));
            ObjectMapper mapper = new ObjectMapper();
            payload.addMetric(String.format("%s/meter/%s", tagPath, "records/hourly"), mapper.writeValueAsString(map));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

    public void createDailyRecord(KuraPayload payload) {

        try {
            get(String.format("%s/meter/%s", tagPath, "yday/volume")).updateValue(get(String.format("%s/meter/%s", tagPath, "today/volume")).getValue());

            HashMap<String, Object> map = new HashMap<>();
            map.put("t_stamp", new Date());
            map.put("id", tagPath);
            map.put("value", get(String.format("%s/meter/%s", tagPath, "yday/volume")));

            ObjectMapper mapper = new ObjectMapper();

            payload.addMetric(String.format("%s/meter/%s", tagPath, "yday/volume"), get(String.format("%s/meter/%s", tagPath, "today/volume")).getValue());
            get(String.format("%s/meter/%s", tagPath, "today/volume")).updateValue((float) 0.0);
            payload.addMetric(String.format("%s/meter/%s", tagPath, "today/volume"), (float) 0.0);

            payload.addMetric(String.format("%s/meter/%s", tagPath, "records/daily"), mapper.writeValueAsString(map));

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}


