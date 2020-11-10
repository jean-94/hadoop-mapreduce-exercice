package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class SortHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private ArrayList<Double> list = new ArrayList<>();
    private ArrayList<Double> listwithoutDuplicate = new ArrayList<>();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        for (DoubleWritable val : values) {
            list.add(val.get());
        }
        Collections.sort(list);
        for (Double val : list) {
            if (!listwithoutDuplicate.contains(val)) {
                listwithoutDuplicate.add(val);
                context.write(new Text(""), new DoubleWritable(val));
            }
        }
    }
}