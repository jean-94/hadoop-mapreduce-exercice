package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
public class DistrictMostTreeReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictMostTreeReducer DistrictMostTreeReducer;

    @Before
    public void setup() {
        this.DistrictMostTreeReducer = new DistrictMostTreeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        IntWritable value1 = new IntWritable(1);
        IntWritable value2 = new IntWritable(2);
        IntWritable value3 = new IntWritable(3);
        List<IntWritable> values = Arrays.asList(value1, value2, value2, value2, value3, value1);
        this.DistrictMostTreeReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(""), value2);

    }
}
