package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TallestTreeSpeciesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();
    private IntWritable height = new IntWritable(1);
    private boolean first = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (!first) {
            String[] itr = value.toString().split(";");
            word.set(itr[3]);
            if(!itr[6].isEmpty()) {
                height.set((int) Double.parseDouble(itr[6]));
            }
            context.write(word, height);
        } else {
            first = false;
        }
    }
}