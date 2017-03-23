/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movielenstop25_chaining;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class Reducer2 extends Reducer<FloatWritable, Text, Text, FloatWritable> {

    int i=0;
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                if(i<25){
                    context.write(value,key);
                }
                i++;
            }
        }
}
