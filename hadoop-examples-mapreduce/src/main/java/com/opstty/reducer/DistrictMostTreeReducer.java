package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DistrictMostTreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private HashMap<IntWritable, Integer> list = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable val : values) {
            if(list.containsKey(val)) {
                list.replace(val, list.get(val) + 1);
            } else {
                list.put(val, 1);
            }
        }

        int resultMax = -1;
        IntWritable result = new IntWritable(-1);

        for (Map.Entry<IntWritable, Integer> entry : list.entrySet()) {
            if (resultMax < entry.getValue()) {
                result = entry.getKey();
            }
        }

        context.write(new Text(""), result);

    }
}