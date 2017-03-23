/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maxUsingWritable;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class Writable_Reducer extends Reducer<Text, WritableObject, Text, WritableObject> {
    
    private WritableObject wo = new WritableObject();
    
    @Override
    public void reduce(Text key, Iterable<WritableObject> values, Context context) throws IOException, InterruptedException
    {
        
        Integer max_stock_volume = 0;
        Integer min_stock_volume = 0;
        Double max_stock_price_adj_close = 0.0;
        
        String min_stock_date;
        String max_stock_date;
        
        wo.setMax_stock_volume(null);
        wo.setMin_stock_volume(null);
        wo.setMax_stock_price_adj_close(null);
        wo.setMin_stock_date("abc");
        wo.setMax_stock_date("xyz");
        
        for(WritableObject val : values){
            max_stock_volume = val.getMax_stock_volume();
            min_stock_volume = val.getMin_stock_volume();
            max_stock_price_adj_close = val.getMax_stock_price_adj_close();
            min_stock_date = val.getMin_stock_date();
            max_stock_date = val.getMax_stock_date();
            
            
            
            if(wo.getMin_stock_volume() == null ||
                    min_stock_volume.compareTo(wo.getMin_stock_volume()) < 0){
                wo.setMin_stock_volume(min_stock_volume);
                wo.setMin_stock_date(min_stock_date);
            }
            
            if(wo.getMax_stock_volume() == null || 
                    max_stock_volume.compareTo(wo.getMax_stock_volume()) > 0){
                wo.setMax_stock_volume(max_stock_volume);
                wo.setMax_stock_date(max_stock_date);
            }
            
            if(wo.getMax_stock_price_adj_close() == null || 
                    max_stock_price_adj_close.compareTo(wo.getMax_stock_price_adj_close()) > 0){
                wo.setMax_stock_price_adj_close(max_stock_price_adj_close);
            }
        }
        context.write(key, wo);
    }
}
