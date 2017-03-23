/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inmemorymedianstddeviation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {

    private IntWritable movie = new IntWritable();
    private FloatWritable rating = new FloatWritable();

    public void map(Object key, Text value, Context context) {
        String row[] = value.toString().split("::");

        try {
            movie.set(Integer.parseInt(row[1]));
            rating.set(Integer.parseInt(row[2]));
            context.write(movie, rating);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
