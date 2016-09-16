package com.cirruslink.example;

import com.cirruslink.example.impl.MemoryFloatValue;
import com.cirruslink.example.model.TagValue;

import java.util.HashMap;

/**
 * Created by kylec on 9/16/2016.
 */
public class Compressor extends HashMap<String, TagValue> {
    public HashMap<Integer,String> registers = new HashMap<>();

    public Compressor(String tagPath) {


        // compressor temperature

        this.put(tagPath + "/compressor/t1", new MemoryFloatValue(24f));
        registers.put(0,tagPath + "/compressor/t1");

        this.put(tagPath + "/compressor/t2", new MemoryFloatValue(30f));
        registers.put(2,tagPath + "/compressor/t2");

        this.put(tagPath + "/compressor/t3", new MemoryFloatValue(-20f));
        registers.put(4,tagPath + "/compressor/t3");

        this.put(tagPath + "/compressor/t4", new MemoryFloatValue(40f));
        registers.put(6,tagPath + "/compressor/t4");

        this.put(tagPath + "/compressor/t5", new MemoryFloatValue(-10f));
        registers.put(8,tagPath + "/compressor/t5");

        this.put(tagPath + "/compressor/t6", new MemoryFloatValue(-50f));
        registers.put(10,tagPath + "/compressor/t6");

        this.put(tagPath + "/compressor/t7", new MemoryFloatValue(18f));
        registers.put(12,tagPath + "/compressor/t7");

        this.put(tagPath + "/compressor/t8", new MemoryFloatValue(95f));
        registers.put(14,tagPath + "/compressor/t8");

        this.put(tagPath + "/compressor/t9", new MemoryFloatValue(85f));
        registers.put(16,tagPath + "/compressor/t9");

        this.put(tagPath + "/compressor/t10", new MemoryFloatValue(66f));
        registers.put(18,tagPath + "/compressor/t10");

        this.put(tagPath + "/compressor/t11", new MemoryFloatValue(100f));
        registers.put(20,tagPath + "/compressor/t11");

        this.put(tagPath + "/compressor/t12", new MemoryFloatValue(88f));
        registers.put(22,tagPath + "/compressor/t12");

        this.put(tagPath + "/compressor/t13", new MemoryFloatValue(-25f));
        registers.put(24,tagPath + "/compressor/t13");

        this.put(tagPath + "/compressor/t14", new MemoryFloatValue(-14f));
        registers.put(26,tagPath + "/compressor/t14");

        this.put(tagPath + "/compressor/t15", new MemoryFloatValue(29f));
        registers.put(28,tagPath + "/compressor/t15");

        this.put(tagPath + "/compressor/t16", new MemoryFloatValue(14f));
        registers.put(30,tagPath + "/compressor/t16");

        this.put(tagPath + "/compressor/t_LT", new MemoryFloatValue(55f));
        registers.put(32,tagPath + "/compressor/t_LT");

        this.put(tagPath + "/compressor/t_RT", new MemoryFloatValue(-26f));
        registers.put(34,tagPath + "/compressor/t_RT");

        // compressor pressure
        this.put(tagPath + "/compressor/p_c1_1", new MemoryFloatValue(50f));
        registers.put(36,tagPath + "/compressor/p_c1_1");

        this.put(tagPath + "/compressor/p_c1_2", new MemoryFloatValue(180f));
        registers.put(38,tagPath + "/compressor/p_c1_2");

        this.put(tagPath + "/compressor/p_c1_3", new MemoryFloatValue(2000f));
        registers.put(40,tagPath + "/compressor/p_c1_3");

        this.put(tagPath + "/compressor/p_c2_1", new MemoryFloatValue(750f));
        registers.put(42,tagPath + "/compressor/p_c2_1");

        this.put(tagPath + "/compressor/p_c2_2", new MemoryFloatValue(250f));
        registers.put(44,tagPath + "/compressor/p_c2_2");

        this.put(tagPath + "/compressor/p_c2_3", new MemoryFloatValue(2880f));
        registers.put(46,tagPath + "/compressor/p_c2_3");

        this.put(tagPath + "/compressor/p_c3_1", new MemoryFloatValue(998f));
        registers.put(48,tagPath + "/compressor/p_c3_1");

        this.put(tagPath + "/compressor/p_c3_2", new MemoryFloatValue(2856f));
        registers.put(50,tagPath + "/compressor/p_c3_2");

        this.put(tagPath + "/compressor/p_c3_3", new MemoryFloatValue(2668f));
        registers.put(52,tagPath + "/compressor/p_c3_3");

        this.put(tagPath + "/compressor/p_c4_1", new MemoryFloatValue(1998f));
        registers.put(54,tagPath + "/compressor/p_c4_1");

        this.put(tagPath + "/compressor/p_c4_2", new MemoryFloatValue(2489f));
        registers.put(56,tagPath + "/compressor/p_c4_2");

        this.put(tagPath + "/compressor/p_c4_3", new MemoryFloatValue(2226f));
        registers.put(58,tagPath + "/compressor/p_c4_3");
    }

}
