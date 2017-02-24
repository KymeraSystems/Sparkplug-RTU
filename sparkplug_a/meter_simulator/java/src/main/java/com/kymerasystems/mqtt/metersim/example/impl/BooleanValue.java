package com.kymerasystems.mqtt.metersim.example.impl;

import com.kymerasystems.mqtt.metersim.example.MeterMain;
import com.kymerasystems.mqtt.metersim.example.model.TagValue;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class BooleanValue implements TagValue<Boolean> {
    private boolean value;

    @Override
    public void setValue(Boolean newValue,boolean flag) {
        this.value = newValue;
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    public boolean updateValue() {
        boolean oldValue = value;
        value = MeterMain.random.nextDouble()>0.99?!value:value;
        return value != oldValue?true:false;
    }

    @Override
    public boolean updateValue(Boolean newValue) {
        this.value = newValue;
        return true;
    }
}
