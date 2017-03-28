/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package structuredtohierarchical;

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
public class StructuredToHierarchical {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PostCommentHierarchy");
        job.setJarByClass(StructuredToHierarchical.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, StructuredToHierarchical_Mapper1.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, StructuredToHierarchical_Mapper2.class);

        job.setReducerClass(StructuredToHierarchical_Reducer1.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }

}
