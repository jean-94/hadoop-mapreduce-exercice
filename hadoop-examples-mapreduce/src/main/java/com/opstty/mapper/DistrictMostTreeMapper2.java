package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMostTreeMapper2 extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text("");
    private Text one = new Text();
    private boolean first = true;

    public void map(Object key, IntWritable value, Context context) throws IOException, InterruptedException {
        if (!first) {
            one = new Text("" + key + "," + value + ";");
            context.write(word, one);
        } else {
            first = false;
        }
    }

}