/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nyse_average;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class NYSE_Average_Mapper extends Mapper<Object,Text,Text,FloatWritable>{
    private Text stock = new Text();
    
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException{
        //StringTokenizer itr = new StringTokenizer(value.toString());
        
        String arr[] = value.toString().split(",");
        try{
        if(!arr[1].equals("stock_symbol")){
            stock.set(new Text(arr[1]));
            
            //System.out.println(stock);
            float stock_high = Float.parseFloat(arr[4]);
            context.write(stock, new FloatWritable(stock_high));
        }
        }
        catch(NumberFormatException e){
            
        }
    }
}
