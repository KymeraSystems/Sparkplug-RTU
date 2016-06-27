package com.cirruslink.example.impl;

import com.cirruslink.example.model.TagValue;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class StringValue implements TagValue<String> {
    String value;


    public StringValue(String initialValue){
        this.value = initialValue;
    }
    @Override
    public void setValue(String newValue,boolean flag) {
        this.value = newValue;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean updateValue() {
        return false;
    }

    @Override
    public boolean updateValue(String newValue) {
        this.value = newValue;
        return true;
    }
}
