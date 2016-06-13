package com.cirruslink.example.impl;

import com.cirruslink.example.SparkplugRaspberryPiExample;
import com.cirruslink.example.model.TagValue;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class MemoryFloatValue implements TagValue<Float> {

    double value;

    public MemoryFloatValue(double initialValue) {

        this.value = initialValue;
    }

    public MemoryFloatValue() {
    }

    @Override
    public void setValue(Float newValue, boolean flag) {
        this.value = newValue;
    }

    @Override
    public Float getValue() {
        return (float) value;
    }

    @Override
    public boolean updateValue() {
        return false;
    }

    @Override
    public boolean updateValue(Float newValue) {
        this.value = newValue.doubleValue();
        return true;
    }
}
