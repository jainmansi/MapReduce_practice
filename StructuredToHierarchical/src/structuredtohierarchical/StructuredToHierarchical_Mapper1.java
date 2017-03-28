/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package structuredtohierarchical;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class StructuredToHierarchical_Mapper1 extends Mapper<Object, Text, Text, Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String seperatedInput[] = value.toString().split(",");
        String movieId = seperatedInput[0];
        
        if (!movieId.equalsIgnoreCase("movieId")) {
            outKey.set(movieId);
            outValue.set("Movie:" + seperatedInput[1]);
            context.write(outKey, outValue);
        }
    }
}
