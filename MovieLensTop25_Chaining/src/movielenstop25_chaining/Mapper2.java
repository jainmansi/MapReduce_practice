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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class Mapper2 extends Mapper<Object, Text, FloatWritable, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String row[] = value.toString().split("\\t");

        Text movieId = new Text(row[0].trim());
        FloatWritable rating = new FloatWritable(Float.parseFloat(row[1].trim()));

        try {
            context.write(rating, movieId);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
