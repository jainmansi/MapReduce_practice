/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package reducerascombiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class AverageReducer extends Reducer<IntWritable, AverageTuple, IntWritable, AverageTuple>{
    
    private AverageTuple result = new AverageTuple();
    
    public void reduce(IntWritable key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException{
        double sum = 0;
        double count = 0;
        
        for(AverageTuple val : values){
            sum += val.getCount() * val.getAverage();
            count += val.getCount();
        }
        
        result.setCount(count);
        result.setAverage(sum/count);
        context.write(key, result);
    }
}
