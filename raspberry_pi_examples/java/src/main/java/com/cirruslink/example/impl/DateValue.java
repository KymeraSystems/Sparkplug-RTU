package com.cirruslink.example.impl;

import com.cirruslink.example.model.TagValue;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by KyleChase on 6/13/2016.
 */
public class DateValue implements TagValue<Long> {

    @Override
    public void setValue(Long newValue, boolean flag) {

        SimpleDateFormat sdf = new SimpleDateFormat("dd MMM yyyy HH:mm:ss");
        try {
            ProcessBuilder pb = new ProcessBuilder("/bin/date","-s",sdf.format(new Date(newValue)));
//            File log = new File("log");
//            pb.redirectErrorStream(true);
//            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
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
        return true;
    }

    @Override
    public boolean updateValue(Long newValue) {
        return true;
    }
}
