package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class SortHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private ArrayList<DoubleWritable> list = new ArrayList<>();
    private ArrayList<DoubleWritable> listwithoutDuplicate = new ArrayList<>();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        for (DoubleWritable val : values) {
            list.add(val);
        }
        Collections.sort(list);
        for (DoubleWritable val : list) {
            context.write(key, val);
            /*
            if (!listwithoutDuplicate.contains(val)) {
                listwithoutDuplicate.add(val);
                context.write(key, val);
            }
             */
        }
    }
}