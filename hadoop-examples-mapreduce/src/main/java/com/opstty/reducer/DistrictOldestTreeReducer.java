package com.opstty.reducer;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictOldestTreeReducer extends Reducer<Text, AgeDistrictWritable, Text, AgeDistrictWritable> {

    private AgeDistrictWritable result = new AgeDistrictWritable();
    private boolean first = true;

    public void reduce(Text key, Iterable<AgeDistrictWritable> values, Context context)
            throws IOException, InterruptedException {

        for (AgeDistrictWritable val : values) {
            if(!first && (val.getAge() > result.getAge())) {
                result = val;
            }else{
                result = val;
                this.first = false;
            }
        }
        context.write(key, result);
    }
}