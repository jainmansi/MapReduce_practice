/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package partitioningpattern;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class PartitioningPattern_Mapper extends Mapper<Object, Text, IntWritable, Text> {

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        try {

            String arr[] = value.toString().split(" ");
            
            // Grab the last access date
            String strDate = arr[3].substring(1,12);
            SimpleDateFormat frmt = new SimpleDateFormat("dd/MMM/yyyy");
            Date date = frmt.parse(strDate);
            // Parse the string into a Calendar object
            Calendar cal = Calendar.getInstance();
            
            cal.setTime(date);
                
            int month = cal.get(Calendar.MONTH) + 1;
            
            context.write(new IntWritable(month), value);
        } catch (ParseException ex) {
            Logger.getLogger(PartitioningPattern_Mapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

