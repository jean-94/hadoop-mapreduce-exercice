package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text word = new Text();
    private NullWritable none = NullWritable.get();
    private boolean first = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (!first) {
            String[] itr = value.toString().split(";");
            word.set(itr[1]);
            context.write(word, none);
        } else {
            first = false;
        }
    }
}