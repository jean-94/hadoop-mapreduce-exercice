package com.opstty.reducer;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictOldestTreeReducer extends Reducer<Text, AgeDistrictWritable, Text, AgeDistrictWritable> {

    private AgeDistrictWritable result = new AgeDistrictWritable();

    public void reduce(Text key, Iterable<AgeDistrictWritable> values, Context context)
            throws IOException, InterruptedException {

        for (AgeDistrictWritable val : values) {
            context.write(key,val);
            /*
            if(val.getAge() > result.getAge()) {
                result = val;
            }*/
        }
    }
}