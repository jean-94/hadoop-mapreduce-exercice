package com.opstty.job;

import com.opstty.AgeDistrictWritable;
import com.opstty.mapper.DistrictOldestTreeMapper;
import com.opstty.reducer.DistrictOldestTreeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictOldestTree {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DistrictOldestTree <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "districtoldesttree");
        job.setJarByClass(DistrictOldestTree.class);
        job.setMapperClass(DistrictOldestTreeMapper.class);
        //job.setCombinerClass(DistrictOldestTreeReducer.class);
        job.setReducerClass(DistrictOldestTreeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AgeDistrictWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

