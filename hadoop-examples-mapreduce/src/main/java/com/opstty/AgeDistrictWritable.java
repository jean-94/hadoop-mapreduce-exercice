package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrictWritable implements Writable {
    private IntWritable district, age;

    public AgeDistrictWritable(){
    }

    public IntWritable getAge() {
        return age;
    }

    public IntWritable getDistrict() {
        return district;
    }

    public void setAge(IntWritable age) {
        this.age = age;
    }

    public void setDistrict(IntWritable district) {
        this.district = district;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        age.write(dataOutput);
        district.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        age.readFields(dataInput);
        district.readFields(dataInput);
    }

    @Override
    public String toString() {
        return district.toString() + " " + age.toString();
    }
}
