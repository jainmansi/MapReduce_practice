/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package innerjoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author mansijain
 */
public class InnerJoin_Reducer2 extends Reducer<Text, Text, Text, Text> {

    public static final Text EMPTY_TEXT = new Text();
    private Text tmp = new Text();

    private ArrayList<Text> listC = new ArrayList<Text>();
    private ArrayList<Text> listD = new ArrayList<Text>();
    private String joinType = null;

   
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listC.clear();
        listD.clear();
        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();
            if (tmp.charAt(0) == 'C') {
                listC.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt(0) == 'D') {
                listD.add(new Text(tmp.toString().substring(1)));
            }

        }

        executeJoinLogic(context);

    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {

        if (!listC.isEmpty() && !listD.isEmpty()) {
            for (Text C : listC) {
                for (Text D : listD) {
                    context.write(C, D);
                }
            }
        }
    }
}
