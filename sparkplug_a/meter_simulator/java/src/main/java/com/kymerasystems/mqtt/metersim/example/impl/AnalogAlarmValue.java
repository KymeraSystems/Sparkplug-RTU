package com.kymerasystems.mqtt.metersim.example.impl;

import com.kymerasystems.mqtt.metersim.example.model.TagValue;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class AnalogAlarmValue implements TagValue<Boolean> {
    private final FloatValue sourceValue;
    private final MemoryBooleanValue en;
    private final MemoryFloatValue sp;
    private final AlarmType alarmType;
    private boolean value;


    public AnalogAlarmValue(MemoryBooleanValue en,FloatValue sourceValue, MemoryFloatValue sp,AlarmType alarmType) {
        this.en = en;
        this.sourceValue = sourceValue;
        this.sp = sp;
        this.alarmType = alarmType;
    }

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
        switch (alarmType){
            case LOW:
                value = this.en.getValue() && this.sourceValue.getValue()<this.sp.getValue();
                break;
            case HIGH:
                value = this.en.getValue() && this.sourceValue.getValue()>this.sp.getValue();
                break;
        }

        return value != oldValue;
    }

    @Override
    public boolean updateValue(Boolean newValue) {
        return false;
    }

    public enum AlarmType{
        HIGH,LOW;
    }
}
