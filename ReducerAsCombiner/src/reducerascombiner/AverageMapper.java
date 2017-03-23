/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package reducerascombiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class AverageMapper extends Mapper<Object, Text, IntWritable, AverageTuple>{
    private IntWritable outYear = new IntWritable();
    private AverageTuple avgTuple = new AverageTuple();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] inputLine = value.toString().split(",");
        
        try{
            if(!inputLine[0].equalsIgnoreCase("exchange")){
                String[] date = inputLine[2].split("/");
                String year = date[2];
                outYear.set(Integer.parseInt(year));
                avgTuple.setAverage(Double.parseDouble(inputLine[8]));
                avgTuple.setCount(1);
                context.write(outYear,avgTuple);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
