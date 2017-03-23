/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maxUsingWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author mansijain
 */
public class WritableObject implements Writable {

    private Integer max_stock_volume;
    private Integer min_stock_volume;
    private Double max_stock_price_adj_close;
    private String max_stock_date;
    private String min_stock_date;
    
    public WritableObject(){
        max_stock_volume = 0;
        min_stock_volume = 0;
        max_stock_price_adj_close = 0.0;
        
    }

    public Integer getMax_stock_volume() {
        return max_stock_volume;
    }

    public void setMax_stock_volume(Integer max_stock_volume) {
        this.max_stock_volume = max_stock_volume;
    }

    public Integer getMin_stock_volume() {
        return min_stock_volume;
    }

    public void setMin_stock_volume(Integer min_stock_volume) {
        this.min_stock_volume = min_stock_volume;
    }

    public String getMax_stock_date() {
        return max_stock_date;
    }

    public void setMax_stock_date(String max_stock_date) {
        this.max_stock_date = max_stock_date;
    }

    public String getMin_stock_date() {
        return min_stock_date;
    }

    public void setMin_stock_date(String min_stock_date) {
        this.min_stock_date = min_stock_date;
    }

    public Double getMax_stock_price_adj_close() {
        return max_stock_price_adj_close;
    }

    public void setMax_stock_price_adj_close(Double max_stock_price_adj_close) {
        this.max_stock_price_adj_close = max_stock_price_adj_close;
    }


    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(max_stock_volume);
        d.writeInt(min_stock_volume);
        d.writeDouble(max_stock_price_adj_close);
        WritableUtils.writeString(d, max_stock_date);
        WritableUtils.writeString(d, min_stock_date);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException, EOFException {
        max_stock_volume = di.readInt();
        min_stock_volume = di.readInt();
        max_stock_price_adj_close = di.readDouble();
        max_stock_date = WritableUtils.readString(di);
        min_stock_date = WritableUtils.readString(di);
    }

    public String toString()
    {
        return (new StringBuilder().append(max_stock_volume).append("\t").
                append(max_stock_date).append("\t").
                append(min_stock_volume).append("\t").
                append(min_stock_date).append("\t").
                append(max_stock_price_adj_close).toString());
    }
    
}
