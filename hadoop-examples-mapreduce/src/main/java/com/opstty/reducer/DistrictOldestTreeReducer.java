package com.opstty.reducer;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictOldestTreeReducer extends Reducer<Text, AgeDistrictWritable, Text, IntWritable> {

    private int result_age = -1;
    private int result_district = -1;

    public void reduce(Text key, Iterable<AgeDistrictWritable> values, Context context)
            throws IOException, InterruptedException {

        for (AgeDistrictWritable val : values) {
            if(val.getAge() > result_age) {
                result_age = val.getAge();
                result_district = val.getDistrict();
            }
        }
        context.write(key, new IntWritable(result_district));
    }
}