/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package partitioningpattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author mansijain
 */
public class PartitioningPattern_Partitioner extends
        Partitioner<IntWritable, Text> implements Configurable {

    private static final String ACCESS_MONTH
            = "access.month";

    private Configuration conf = null;
    private int accessMonth = 0;

    public int getPartition(IntWritable key, Text value, int numPartitions){
        return key.get() - accessMonth;
    }

    public Configuration getConf() {
        return conf;
    }
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        accessMonth = conf.getInt(ACCESS_MONTH, 0);
    }

    public static void setAccessMonth(Job job, int accessMonth){
        job.getConfiguration().setInt(ACCESS_MONTH, accessMonth);
    }

    
}
