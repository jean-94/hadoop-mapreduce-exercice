package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TallestTreeSpeciesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable(1.0);

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double height = 0.0;
        for (DoubleWritable val : values) {
            if(height <= val.get()) {
                height = val.get();
            }
        }
        result.set(height);
        context.write(key, result);
    }
}