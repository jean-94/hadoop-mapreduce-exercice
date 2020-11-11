package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
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

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SortHeightReducerTest {
    @Mock
    private Reducer.Context context;
    private SortHeightReducer SortHeightReducer;

    @Before
    public void setup() {
        this.SortHeightReducer = new SortHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        DoubleWritable value1 = new DoubleWritable(1.0);
        DoubleWritable value2 = new DoubleWritable(2.0);
        DoubleWritable value3 = new DoubleWritable(2.1);
        Iterable<DoubleWritable> values = Arrays.asList(value1, value3, value2);
        this.SortHeightReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(""), value1);
        verify(this.context).write(new Text(""), value2);
        verify(this.context).write(new Text(""), value3);
    }
}
