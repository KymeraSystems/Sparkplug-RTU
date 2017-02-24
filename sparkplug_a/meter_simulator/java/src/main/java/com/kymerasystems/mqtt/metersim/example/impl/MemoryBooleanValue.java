package com.kymerasystems.mqtt.metersim.example.impl;

import com.kymerasystems.mqtt.metersim.example.model.TagValue;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class MemoryBooleanValue implements TagValue<Boolean> {

    boolean value;

    public MemoryBooleanValue(boolean initialValue) {

        this.value = initialValue;
    }

    public MemoryBooleanValue() {
    }

    @Override
    public void setValue(Boolean newValue, boolean flag) {
        this.value = newValue;
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    public boolean updateValue() {
        return false;
    }

    @Override
    public boolean updateValue(Boolean newValue) {
        this.value = newValue.booleanValue();
        return true;
    }
}
