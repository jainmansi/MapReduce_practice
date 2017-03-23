/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inmemorymedianstddeviation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class MedianStdDevReducer extends Reducer<IntWritable, FloatWritable, IntWritable, MedianStdDevTuple> {

    private MedianStdDevTuple result = new MedianStdDevTuple();
    private ArrayList<Float> ratings = new ArrayList<Float>();

    public void reduce(IntWritable key, Iterable<FloatWritable> values,
            Context context) throws IOException, InterruptedException {

        float sum = 0;
        float count = 0;
        ratings.clear();
        result.setStddev(0);

        for (FloatWritable val : values) {
            ratings.add((float) val.get());
            sum += val.get();
            ++count;
        }

        Collections.sort(ratings);

        if (count % 2 == 0) {
            result.setMedian((ratings.get((int) count / 2 - 1) + ratings
                    .get((int) count / 2)) / 2.0f);
        } else {
            result.setMedian(ratings.get((int) count / 2));
        }

        float mean = sum / count;

        float sumOfSquares = 0.0f;
        for (Float f : ratings) {
            sumOfSquares += (f - mean) * (f - mean);
        }

        result.setStddev((float) Math.sqrt(sumOfSquares / (count - 1)));

        context.write(key, result);
    }
}
