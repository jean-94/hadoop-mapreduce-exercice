package com.opstty;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrictWritable implements Writable {
    private int district, age;

    public AgeDistrictWritable(){
    }

    public int getAge() {
        return age;
    }

    public int getDistrict() {
        return district;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setDistrict(int district) {
        this.district = district;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(age);
        dataOutput.writeInt(district);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.age = dataInput.readInt();
        this.district = dataInput.readInt();
    }

    @Override
    public String toString() {
        return Integer.toString(this.district);
    }
}
