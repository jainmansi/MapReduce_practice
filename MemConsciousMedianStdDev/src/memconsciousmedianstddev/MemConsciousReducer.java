/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package memconsciousmedianstddev;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class MemConsciousReducer extends Reducer<IntWritable, SortedMapWritable,IntWritable, MedianStdDevTuple>{
    private MedianStdDevTuple result = new MedianStdDevTuple();
    private TreeMap<Double, Long> movieRatings = new TreeMap<Double, Long>();

    @Override
    protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
        
        double sum = 0;
        double totalRatings = 0;
        movieRatings.clear();
        result.setMedian(0);
        result.setStandardDeviation(0);
        
        for(SortedMapWritable v : values){
            for(Map.Entry<WritableComparable, Writable> entry : v.entrySet()){
                //int rating = ((IntWritable) entry.getKey()).get();
                Double rating = ((DoubleWritable) entry.getKey()).get();
                Long count = ((LongWritable) entry.getValue()).get();
                
                totalRatings += count;
                sum += rating * count;
                
                Long storedCount = movieRatings.get(rating);
                if(storedCount == null){
                    movieRatings.put(rating, count);
                }
                else{
                    movieRatings.put(rating, storedCount+count);
                }
            }
        }
        
        double medianIndex = totalRatings / 2L;
        double prevRating = 0;
        double ratings = 0;
        double prevKey = 0;
        
        for(Map.Entry<Double, Long> entry : movieRatings.entrySet()){
            ratings = prevRating + entry.getValue();
            
            if(prevRating <= medianIndex && medianIndex < ratings){
                if(totalRatings % 2 == 0 && prevRating == medianIndex){
                    result.setMedian((double) (entry.getKey() + prevKey) / 2.0);
                }else{
                    result.setMedian(entry.getKey());
                }
                break;
            }
            
            prevRating = ratings;
            prevKey = entry.getKey();
        }
        
        double mean = sum / totalRatings;
        double sumOfSquares = 0;
        
        for(Map.Entry<Double, Long> entry : movieRatings.entrySet()){
            sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
        }
        
        result.setStandardDeviation((double)Math.sqrt(sumOfSquares / (totalRatings - 1)));
        
        context.write(key, result);
    }
    
    
}

