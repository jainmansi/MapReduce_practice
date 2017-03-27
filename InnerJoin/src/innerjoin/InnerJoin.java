/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package innerjoin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author mansijain
 */
public class InnerJoin {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inner_join");
        job.setJarByClass(InnerJoin.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, InnerJoin_Mapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, InnerJoin_Mapper2.class);

        job.setReducerClass(InnerJoin_Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean complete = job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "inner_join");
        if (complete) {
            job2.setJarByClass(InnerJoin.class);
            MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, InnerJoin_Mapper3.class);
            MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, InnerJoin_Mapper4.class);

            job2.setReducerClass(InnerJoin_Reducer2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job2, new Path(args[4]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}
