package com.opstty.mapper;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;

public class DistrictOldestTreeMapper extends Mapper<Object, Text, Text, AgeDistrictWritable> {
    private Text word = new Text(" ");
    private AgeDistrictWritable ageDistrictValue = new AgeDistrictWritable();
    private boolean first = true;
    private int actual_year = Calendar.getInstance().get(Calendar.YEAR);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (!first) {
            String[] itr = value.toString().split(";");
            if(!itr[5].isEmpty()) {
                ageDistrictValue.setDistrict(Integer.parseInt(itr[1]));
                ageDistrictValue.setAge(actual_year - Integer.parseInt(itr[5]));
                context.write(word, ageDistrictValue);
            }
        } else {
            first = false;
        }
    }
}