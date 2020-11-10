package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TallestTreeSpeciesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int height = 0;
        for (IntWritable val : values) {
            if(height <= val.get()) {
                height = val.get();
            }
        }
        result.set(height);
        context.write(key, result);
    }
}