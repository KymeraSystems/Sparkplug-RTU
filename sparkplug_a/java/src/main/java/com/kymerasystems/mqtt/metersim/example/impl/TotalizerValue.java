package com.kymerasystems.mqtt.metersim.example.impl;

import com.kymerasystems.mqtt.metersim.example.MeterMain;
import com.kymerasystems.mqtt.metersim.example.model.TagValue;

import java.util.Calendar;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class TotalizerValue implements TagValue<Float> {

    private final Float deadBand;
    private double delta;
    double value;
    private Float lastSent = 0.0f;
    private final long updateRate;
    private boolean enabled = true;
    private int lastUpdate = -1;


    public TotalizerValue(long updateRate, Float deadBand) {

        this.updateRate = updateRate;
        this.delta = MeterMain.random.nextFloat() * 0.1;
        this.value = MeterMain.random.nextFloat() * 100;
        this.deadBand = deadBand;
    }

    @Override
    public void setValue(Float newValue, boolean flag) {
        double value = newValue;
        if (value > 0.0) {
            this.delta = value * updateRate / 86400000L;
        } else if (value == 0.0) {
            this.value = 0.0;
        } else {
            enabled = enabled ? false : true;
        }
    }

    @Override
    public Float getValue() {
        return (float) value;
    }

    @Override
    public boolean updateValue() {
        Calendar c = Calendar.getInstance();

        if (enabled) {
            double rand = MeterMain.random.nextDouble();
            double sub = delta * 0.03 * rand;
            value += delta + sub;
        }
        if (c.get(Calendar.MINUTE) != lastUpdate) {
            lastSent = (float) value;
            lastUpdate = c.get(Calendar.MINUTE);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean updateValue(Float newValue) {

        this.value = newValue;
        return true;
    }
}
