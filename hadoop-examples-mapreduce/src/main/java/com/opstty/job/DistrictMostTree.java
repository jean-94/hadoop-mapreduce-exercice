package com.opstty.job;

import com.opstty.AgeDistrictWritable;
import com.opstty.mapper.DistrictMostTreeMapper;
import com.opstty.mapper.DistrictMostTreeMapper2;
import com.opstty.mapper.DistrictOldestTreeMapper;
import com.opstty.reducer.DistrictMostTreeReducer;
import com.opstty.reducer.DistrictMostTreeReducer2;
import com.opstty.reducer.DistrictOldestTreeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictMostTree {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DistrictMostTree <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "districtmosttrees");
        job.setJarByClass(DistrictMostTree.class);
        job.setMapperClass(DistrictMostTreeMapper.class);
        job.setCombinerClass(DistrictMostTreeReducer.class);
        job.setReducerClass(DistrictMostTreeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        Path outputpath = new Path("Temp");
        FileOutputFormat.setOutputPath(job,outputpath);
        outputpath.getFileSystem(conf).delete(outputpath);
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "districtmosttrees2");
        job2.setJarByClass(DistrictMostTree.class);
        job2.setMapperClass(DistrictMostTreeMapper2.class);
        job2.setReducerClass(DistrictMostTreeReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, outputpath);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}

