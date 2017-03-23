/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movielenstop25_chaining;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class Mapper1 extends Mapper<Object, Text, Text, FloatWritable>{
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String row[] = value.toString().split("::");
        
        Text movieId = new Text(row[1]);
        FloatWritable rating = new FloatWritable(Float.parseFloat(row[2]));
        
        try{
            context.write(movieId, rating);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
