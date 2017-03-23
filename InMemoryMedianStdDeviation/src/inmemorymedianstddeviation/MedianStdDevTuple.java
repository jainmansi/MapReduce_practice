/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inmemorymedianstddeviation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author mansijain
 */
public class MedianStdDevTuple implements Writable {

    private float median = 0;
    private float stddev = 0f;

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public float getStddev() {
        return stddev;
    }

    public void setStddev(float stddev) {
        this.stddev = stddev;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(median);
        out.writeFloat(stddev);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        median = in.readFloat();
        stddev = in.readFloat();
    }

    @Override
    public String toString() {
        return (median + "\t" + stddev);
    }

}
