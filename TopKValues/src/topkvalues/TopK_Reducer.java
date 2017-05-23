/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topkvalues;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class TopK_Reducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    private static int TOP_NUM = 10;
    private static final TreeMap<LongWritable, Text> TopKMap = new TreeMap<>();

    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            Long keyval = new Long(key.toString());
            TopKMap.put(new LongWritable(keyval), new Text(value));
        }

        if (TopKMap.size() > TOP_NUM) {
            TopKMap.remove(TopKMap.firstKey());
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (LongWritable k : TopKMap.keySet()) {
            context.write(new Text(TopKMap.get(k)), k);
        }
    }

}
