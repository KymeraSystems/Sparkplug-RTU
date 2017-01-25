package com.kymerasystems.mqtt.metersim.example.model;

/**
 * Created by KyleChase on 6/12/2016.
 */
public interface TagValue<T> {

    void setValue(T newValue, boolean flag);
    T getValue();
    boolean updateValue();
    boolean updateValue(T newValue);

}
