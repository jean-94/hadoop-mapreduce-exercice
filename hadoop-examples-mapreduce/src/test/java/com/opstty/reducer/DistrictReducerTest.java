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

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistrictReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictReducer DistrictReducer;

    @Before
    public void setup() {
        this.DistrictReducer = new DistrictReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        NullWritable none = NullWritable.get();
        Iterable<NullWritable> values = Arrays.asList(none, none, none);
        this.DistrictReducer.reduce(new Text(key), values , this.context);
        verify(this.context).write(new Text(key), none);
    }
}

