package com.kymerasystems.mqtt.metersim.example.impl;

import com.kymerasystems.mqtt.metersim.example.model.TagValue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class DateValue implements TagValue<Long> {
    private int lastUpdate = 0;
    @Override
    public void setValue(Long newValue, boolean flag) {

        SimpleDateFormat sdf = new SimpleDateFormat("dd MMM yyyy HH:mm:ss");
        try {
            ProcessBuilder pb = new ProcessBuilder("/bin/date", "-s", sdf.format(new Date(newValue)));
            pb.start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Long getValue() {
        return new Date().getTime();
    }

    @Override
    public boolean updateValue() {

        Calendar c = Calendar.getInstance();
        if (c.get(Calendar.HOUR) != lastUpdate)
        {
            lastUpdate = c.get(Calendar.HOUR);
            return true;
        }
        return false;
    }

        @Override
        public boolean updateValue (Long newValue){
            return true;
        }

}
