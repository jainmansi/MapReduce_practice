/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package memconsciousmedianstddev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author mansijain
 */
public class MedianStdDevTuple implements Writable{

    private double median;
    private double standardDeviation;

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeDouble(median);
        d.writeDouble(standardDeviation);
    }
    @Override
    public void readFields(DataInput di) throws IOException {
        median = di.readDouble();
        standardDeviation = di.readDouble();
    }
    
    public String toString(){
        return (this.getMedian() + "::" + this.getStandardDeviation());
    }
}
