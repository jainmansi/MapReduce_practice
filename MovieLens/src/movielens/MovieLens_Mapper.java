package movielens;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author mansijain
 */
public class MovieLens_Mapper extends Mapper<Object,Text,Text,IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text userId = new Text();
    
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException{
        StringTokenizer itr = new StringTokenizer(value.toString());

        userId.set(itr.nextToken("::"));
            
        System.out.println(userId);
        context.write(userId, one);
        
    }
}
