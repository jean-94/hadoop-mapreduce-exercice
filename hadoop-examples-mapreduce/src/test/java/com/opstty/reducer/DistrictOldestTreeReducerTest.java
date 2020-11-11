package com.opstty.reducer;

import com.opstty.AgeDistrictWritable;
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
public class DistrictOldestTreeReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictOldestTreeReducer DistrictOldestTreeReducer;

    @Before
    public void setup() {
        this.DistrictOldestTreeReducer = new DistrictOldestTreeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        NullWritable none = NullWritable.get();
        AgeDistrictWritable ageDistrictWritable1 = new AgeDistrictWritable();
        ageDistrictWritable1.setDistrict(7);
        ageDistrictWritable1.setAge(85);
        AgeDistrictWritable ageDistrictWritable2 = new AgeDistrictWritable();
        ageDistrictWritable1.setDistrict(8);
        ageDistrictWritable1.setAge(90);
        List<AgeDistrictWritable> values = Arrays.asList(ageDistrictWritable1, ageDistrictWritable2);
        this.DistrictOldestTreeReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new IntWritable(8));

    }
}
