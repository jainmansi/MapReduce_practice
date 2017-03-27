/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package innerjoin;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class InnerJoin_Mapper2 extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String uid = value.toString().split(";")[0].trim();
        
        if (!uid.equalsIgnoreCase("User-ID")) {
            outKey.set(uid);
            outValue.set("B" + value);
            context.write(outKey, outValue);
        }
    }
}
