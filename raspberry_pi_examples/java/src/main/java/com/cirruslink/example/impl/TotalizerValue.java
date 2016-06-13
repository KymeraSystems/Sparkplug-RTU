package com.cirruslink.example.impl;

import com.cirruslink.example.SparkplugRaspberryPiExample;
import com.cirruslink.example.model.TagValue;

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


    public TotalizerValue(long updateRate,Float deadBand) {

        this.updateRate = updateRate;
        this.delta = SparkplugRaspberryPiExample.random.nextFloat() * 0.1;
        this.value = SparkplugRaspberryPiExample.random.nextFloat() * 100;
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
        if (enabled) {
            double rand = SparkplugRaspberryPiExample.random.nextDouble();
            double sub = delta * 0.03 * rand;
            value += delta + sub;
        }
        if (Math.abs(value-lastSent)> deadBand){
            lastSent = (float) value;
            return true;
        } else{
            return false;
        }
    }

    @Override
    public boolean updateValue(Float newValue) {
        this.value = newValue;
        return true;
    }
}
