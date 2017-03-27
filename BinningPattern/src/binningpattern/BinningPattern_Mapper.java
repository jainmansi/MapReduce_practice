/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package binningpattern;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author mansijain
 */
public class BinningPattern_Mapper extends
        Mapper<Object, Text, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> mos = null;

    protected void setup(Context context) {
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String arr[] = value.toString().split(" ");
            
            // Grab the last access date
            String time = arr[3].substring(13,15);
            
            int t = Integer.parseInt(time);
        
            if (t==1) {
                mos.write("bins", value, NullWritable.get(), "01");
            }
            if (t==2) {
                mos.write("bins", value, NullWritable.get(), "02");
            }
            if (t==3) {
                mos.write("bins", value, NullWritable.get(), "03");
            }
            if (t==4) {
                mos.write("bins", value, NullWritable.get(), "04");
            }
            if (t==5) {
                mos.write("bins", value, NullWritable.get(), "05");
            }
            if (t==6) {
                mos.write("bins", value, NullWritable.get(), "06");
            }
            if (t==7) {
                mos.write("bins", value, NullWritable.get(), "07");
            }
            if (t==8) {
                mos.write("bins", value, NullWritable.get(), "08");
            }
            if (t==9) {
                mos.write("bins", value, NullWritable.get(), "09");
            }
            if (t==10) {
                mos.write("bins", value, NullWritable.get(), "10");
            }
            if (t==11) {
                mos.write("bins", value, NullWritable.get(), "11");
            }
            if (t==12) {
                mos.write("bins", value, NullWritable.get(), "12");
            }
            if (t==13) {
                mos.write("bins", value, NullWritable.get(), "13");
            }
            if (t==14) {
                mos.write("bins", value, NullWritable.get(), "14");
            }
            if (t==15) {
                mos.write("bins", value, NullWritable.get(), "15");
            }
            if (t==16) {
                mos.write("bins", value, NullWritable.get(), "16");
            }
            if (t==17) {
                mos.write("bins", value, NullWritable.get(), "17");
            }
            if (t==18) {
                mos.write("bins", value, NullWritable.get(), "18");
            }
            if (t==19) {
                mos.write("bins", value, NullWritable.get(), "19");
            }
            if (t==20) {
                mos.write("bins", value, NullWritable.get(), "20");
            }
            if (t==21) {
                mos.write("bins", value, NullWritable.get(), "21");
            }
            if (t==22) {
                mos.write("bins", value, NullWritable.get(), "22");
            }
            if (t==23) {
                mos.write("bins", value, NullWritable.get(), "23");
            }
            if (t==0) {
                mos.write("bins", value, NullWritable.get(), "24");
            }
        
    }

    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // Close multiple outputs!
        mos.close();
    }
}
