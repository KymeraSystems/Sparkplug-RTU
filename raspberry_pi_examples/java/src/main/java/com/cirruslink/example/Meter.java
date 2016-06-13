package com.cirruslink.example;

import com.cirruslink.example.impl.BooleanValue;
import com.cirruslink.example.impl.FloatValue;
import com.cirruslink.example.impl.MemoryFloatValue;
import com.cirruslink.example.impl.TotalizerValue;
import com.cirruslink.example.model.TagValue;
import org.eclipse.kura.message.KuraPayload;

import java.util.*;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class Meter extends HashMap<String,TagValue> {
    String tagPath;

    public Meter(String tagPath) {
        this.tagPath = tagPath;
        this.put(String.format("%s/meter/%s",tagPath,"sp"),new FloatValue(0.0f,2000.0f,0.1f,1.0f));
        this.put(String.format("%s/meter/%s",tagPath,"dp"),new FloatValue(0.0f,200.0f,0.1f,0.2f));
        this.put(String.format("%s/meter/%s",tagPath,"temp"),new FloatValue(0.0f,100.0f,0.1f,0.4f));
        this.put(String.format("%s/rtu/%s",tagPath,"volts"),new FloatValue(0.0f,12.0f,0.1f,0.3f));
        this.put(String.format("%s/rtu/%s",tagPath,"temp"),new FloatValue(0.0f,100.0f,0.1f,0.3f));
        this.put(String.format("%s/meter/%s",tagPath,"flowRate"),new FloatValue(0.0f,500.0f,0.1f,0.5f));
        this.put(String.format("%s/%s",tagPath,"casing pressure"),new FloatValue(0.0f,2000.0f,0.1f,5.0f));
        this.put(String.format("%s/%s",tagPath,"esd valve"),new BooleanValue());
        this.put(String.format("%s/meter/%s",tagPath,"today/volume"),new TotalizerValue(1000l,0.01f));
        this.put(String.format("%s/meter/%s",tagPath,"yday/volume"),new MemoryFloatValue());
        this.put(String.format("%s/meter/%s",tagPath,"records/hourly"),new MemoryFloatValue());
        this.put(String.format("%s/meter/%s",tagPath,"records/daily"),new MemoryFloatValue());
        this.put(String.format("%s/meter/analysis/%s",tagPath,"co2"),new MemoryFloatValue(3.11));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"n2"),new MemoryFloatValue(2.2));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c1"),new MemoryFloatValue(93.34));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c2"),new MemoryFloatValue(0.55));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c3"),new MemoryFloatValue(0.2));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"ic4"),new MemoryFloatValue(0.07));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"nc4"),new MemoryFloatValue(0.07));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"ic5"),new MemoryFloatValue(0.03));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"nc5"),new MemoryFloatValue(0.05));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c6"),new MemoryFloatValue(0.07));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c7"),new MemoryFloatValue(0.07));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c8"),new MemoryFloatValue(0.08));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c9"),new MemoryFloatValue(0.03));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"c10"),new MemoryFloatValue(0.01));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"h2o"),new MemoryFloatValue(0.0));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"h2s"),new MemoryFloatValue(0.0));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"h2"),new MemoryFloatValue(0.03));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"co"),new MemoryFloatValue(0.0));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"he"),new MemoryFloatValue(0.09));
        this.put(String.format("%s/meter/analysis/%s",tagPath,"o2"),new MemoryFloatValue(0.0));

    }

    public void updateMeter(KuraPayload payload,boolean init){

        Calendar c = Calendar.getInstance();

        for (Entry<String,TagValue> e:this.entrySet()){
            if (init || e.getValue().updateValue()){
                payload.addMetric(e.getKey(),e.getValue().getValue());
            }
        }

        if (c.get(Calendar.HOUR_OF_DAY)==0){
            get(String.format("%s/meter/%s",tagPath,"yday/volume")).updateValue(get(String.format("%s/meter/%s",tagPath,"today/volume")).getValue());
            payload.addMetric(String.format("%s/meter/%s",tagPath,"yday/volume"),get(String.format("%s/meter/%s",tagPath,"today/volume")).getValue());
            get(String.format("%s/meter/%s",tagPath,"today/volume")).updateValue(0.0);
            payload.addMetric(String.format("%s/meter/%s",tagPath,"today/volume"),0.0);
        }

        if (c.get(Calendar.MINUTE)==0){
            get(String.format("%s/meter/%s",tagPath,"records/hourly")).updateValue((float)c.get(Calendar.HOUR_OF_DAY));
            payload.addMetric(String.format("%s/meter/%s",tagPath,"records/hourly"),(float)c.get(Calendar.HOUR_OF_DAY));
        }
    }

}
