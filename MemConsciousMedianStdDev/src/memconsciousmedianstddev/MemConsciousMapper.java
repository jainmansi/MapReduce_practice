/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package memconsciousmedianstddev;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class MemConsciousMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable>{
    
    private static final LongWritable one = new LongWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String record[] = value.toString().split("::");
                
        String movie = record[1];
        String rating = record[2];
        
        IntWritable movieId = new IntWritable(Integer.parseInt(movie));
        DoubleWritable ratings = new DoubleWritable(Double.parseDouble(rating));
                    
        SortedMapWritable output = new SortedMapWritable();
        output.put(ratings,one);
        
        context.write(movieId,output);        
    }
    
                
}