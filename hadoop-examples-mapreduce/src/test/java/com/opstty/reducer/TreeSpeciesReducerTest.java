package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TreeSpeciesReducerTest {
    @Mock
    private Reducer.Context context;
    private TreeSpeciesReducer TreeSpeciesReducer;

    @Before
    public void setup() {
        this.TreeSpeciesReducer = new TreeSpeciesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        NullWritable none = NullWritable.get();
        IntWritable value1 = new IntWritable(1);
        List<IntWritable> values = Arrays.asList(value1, value1, value1, value1);
        this.TreeSpeciesReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), none);

    }
}
