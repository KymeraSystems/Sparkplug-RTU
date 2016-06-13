package com.cirruslink.example.impl;

import com.cirruslink.example.SparkplugRaspberryPiExample;
import com.cirruslink.example.model.TagValue;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class IntegerValue implements TagValue<Integer> {
    private final Float lowValue;
    private final Float highValue;
    private final Float variance;
    private final Float deadBand;
    double value;
    private Float target;
    private double lastSent;


    public IntegerValue(Float lowValue, Float highValue, Float variance, Float deadBand) {

        this.lowValue = lowValue;
        this.highValue = highValue;
        this.variance = variance;
        this.deadBand = deadBand;
    }

    @Override
    public void setValue(Integer newValue, boolean flag) {
        this.target = (float) newValue;
        if (flag) {
            this.value = newValue;
        }
    }

    @Override
    public Integer getValue() {
        return (int) value;
    }

    @Override
    public boolean updateValue() {
        double rand = SparkplugRaspberryPiExample.random.nextDouble();
        double sub = (target > (highValue-lowValue)/2.0) ? (highValue-lowValue - target) : target+lowValue;
        double v2 = rand + (target - value) / sub - 0.5;
        value = value + v2;
        if (Math.abs(value-lastSent)> deadBand){
            lastSent = (float) value;
            return true;
        } else{
            return false;
        }
    }

    @Override
    public boolean updateValue(Integer newValue) {
        this.value = newValue;
        return true;
    }
}
