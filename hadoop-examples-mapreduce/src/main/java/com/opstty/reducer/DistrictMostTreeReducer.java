package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class DistrictMostTreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private ArrayList<Integer> list_district = new ArrayList<>();
    private ArrayList<Integer> list_counters = new ArrayList<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable val : values) {
            if( this.list_district.contains(val.get())){
                addToKey(val.get());
            }
            else{
                this.list_district.add(val.get());
                this.list_counters.add(1);
            }
        }
        int iresutl = i_max_counter();
        context.write(new Text(""), new IntWritable(this.list_district.get(iresutl)));
    }

    private int i_max_counter(){
        int imax = 0;
        int max = this.list_counters.get(0);
        for(int i = 0; i < this.list_counters.size(); i++){
            if (max < this.list_counters.get(i)){
                max = this.list_counters.get(i);
                imax = i;
            }
        }
        return imax;
    }

    private void addToKey(int key){
        for (int i = 0; i < this.list_district.size(); i++){
            if( this.list_district.get(i) == key){
                this.list_counters.set(i, this.list_counters.get(i) + 1);
            }
        }
    }

}