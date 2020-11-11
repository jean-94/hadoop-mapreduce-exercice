package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMostTreeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private IntWritable word = new IntWritable();
    private boolean first = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (!first) {
            String[] itr = value.toString().split(";");
            word.set(Integer.parseInt(itr[1]));
            context.write(new Text(""), word);
        } else {
            first = false;
        }
    }

}