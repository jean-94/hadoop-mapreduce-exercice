package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictMostTreeReducer2 extends Reducer<Text, Text, Text, Text> {
    private IntWritable one = new IntWritable(1);
    private boolean first = true;
    private String result;
    private int max;

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            String[] itr = val.toString().split(",");
            if(first) {
                result = itr[0];
                max = Integer.parseInt(itr[1]);
            } else if (max < Integer.parseInt(itr[1])) {
                first = false;
                result = itr[0];
                max = Integer.parseInt(itr[1]);
            }
        }
        context.write(key, new Text(result));
    }
}