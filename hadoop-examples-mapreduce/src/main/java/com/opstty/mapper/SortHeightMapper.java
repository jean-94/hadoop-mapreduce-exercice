package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text word = new Text(" ");
    private DoubleWritable height = new DoubleWritable(1);
    private boolean first = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (!first) {
            String[] itr = value.toString().split(";");
            if(!itr[6].isEmpty()) {
                height.set(Double.parseDouble(itr[6]));
            }
            context.write(word, height);
        } else {
            first = false;
        }
    }
}