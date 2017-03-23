/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nyse_average;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class NYSE_Average_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
    
    private FloatWritable result = new FloatWritable();
    
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
        
        int sum = 0;
        int count = 0;
        for(FloatWritable val : values){
            sum += val.get();
            count++;
        }
        result.set(sum/count);
        System.out.println(result);
        context.write(key,result);
    }
}
