/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movielenstop25_chaining;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author nirmal
 */
public class RatingComparator extends WritableComparator{
    
    public RatingComparator(){
        super(FloatWritable.class,true);
        
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      
        FloatWritable ip1 = (FloatWritable) w1;
        FloatWritable ip2 = (FloatWritable) w2;
        int cmp = -1 * ip1.compareTo(ip2);

        return cmp;
    }
    
    
}
