/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topkvalues;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class TopK_Mapper extends Mapper<Object, Text, LongWritable, Text> {

    private TreeMap<String, Long> stationCount = new TreeMap<>();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String separatedInputs[] = value.toString().split(",");
        IntWritable one = new IntWritable(1);
        String station = separatedInputs[3];
        
        if(stationCount.containsKey(station)){
            stationCount.put(station, stationCount.get(station) + 1);
        }
        
        else{
            stationCount.put(station, new Long(1));
        }
        
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String k : stationCount.keySet()) {
            LongWritable total = new LongWritable(stationCount.get(k));
            context.write(total, new Text(k));
        }
    }
}
