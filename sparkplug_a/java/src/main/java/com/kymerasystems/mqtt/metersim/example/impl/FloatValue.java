package com.kymerasystems.mqtt.metersim.example.impl;

import com.kymerasystems.mqtt.metersim.example.MeterMain;
import com.kymerasystems.mqtt.metersim.example.model.TagValue;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class FloatValue implements TagValue<Float> {
    private final Float lowValue;
    private final Float highValue;
    private final Float variance;
    private final Float deadBand;
    float value = 0.0f;
    private Float target;
    private Float lastSent = 0.0f;


    public FloatValue(Float lowValue, Float highValue, Float variance, Float deadBand) {
        this.lowValue = lowValue;
        this.highValue = highValue;
        this.variance = variance;
        this.deadBand = deadBand;
        this.target = (float) MeterMain.random.nextDouble() * (highValue - lowValue) + lowValue;

    }

    @Override
    public void setValue(Float newValue, boolean flag) {
        this.target = newValue;
        if (flag) {
            this.value = newValue;
        }
    }

    @Override
    public Float getValue() {
        return value;
    }

    @Override
    public boolean updateValue() {
        float rand = MeterMain.random.nextFloat();
        float sub = (target > (highValue - lowValue) / 2.0) ? (highValue - lowValue - target) : target + lowValue;
        float v2 = (rand*variance + ((target - value) / sub)) - (variance/2);
        value = value + v2;
        if (Math.abs(value - lastSent) > deadBand) {
            lastSent = value;
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
