/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package distinctpattern;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class DistinctPattern_Mapper extends Mapper<Object, Text, Text, NullWritable>{
    private Text outIP = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        
        StringTokenizer itr = new StringTokenizer(value.toString());
        
        outIP.set(itr.nextToken());
        context.write(outIP, NullWritable.get());
        
    }
}
