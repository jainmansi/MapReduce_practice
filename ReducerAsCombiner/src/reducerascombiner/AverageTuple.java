/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package reducerascombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author mansijain
 */
public class AverageTuple implements Writable{
    
    private double count;
    private double average;

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeDouble(count);
        d.writeDouble(average);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        count = di.readDouble();
        average = di.readDouble();
    }
    
    @Override
    public String toString() {
        return count+"\t"+average;
    }
    
}
