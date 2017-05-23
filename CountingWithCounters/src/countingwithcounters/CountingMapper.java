/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package countingwithcounters;

import java.io.IOException;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class CountingMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static String STR = "month";
    IntWritable one = new IntWritable(1);
    String monthString = "";
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String separatedInputs[] = value.toString().split(",");
        monthString = "";
        if (!separatedInputs[1].equalsIgnoreCase("Duration")) {
            try {
                String strDate = separatedInputs[2].split(" ")[0];
                SimpleDateFormat frmt = new SimpleDateFormat("MM/dd/yyyy");
                Date date = frmt.parse(strDate);
            // Parse the string into a Calendar object
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                int month = cal.get(Calendar.MONTH) + 1;
                monthString = intToMonth(month - 1);
                monthString = removeSpaces(monthString);
                context.write(new Text(monthString), one);
                Counter counter = context.getCounter(STR, monthString);
                counter.increment(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private String removeSpaces(String str) {
        
        if (str.startsWith("(")) {
            str = str.substring(1);
        }
        if (str.endsWith(")")) {
            str = str.substring(0, str.length() - 2);
        }
        if (str.endsWith(".") || str.endsWith(",")) {
            str = str.substring(0, str.length() - 2);
        }
        return str;
    }

    private String intToMonth(int num) {
        String month = "incorrect";
        DateFormatSymbols dateFrmt = new DateFormatSymbols();
        String[] months = dateFrmt.getMonths();
        if (num >= 0 && num <= 11) {
            month = months[num];
        }
        return month;
    }
}
