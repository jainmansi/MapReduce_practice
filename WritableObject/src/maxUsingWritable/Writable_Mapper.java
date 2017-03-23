/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maxUsingWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author mansijain
 */
public class Writable_Mapper extends Mapper<Object, Text, Text, WritableObject> {

    private String max_stock_date;
    private String min_stock_date;
    private Integer max_stock_volume;
    private Integer min_stock_volume;
    private Double max_stock_price_adj_close;

    private WritableObject wo = new WritableObject();

    @Override
    public void map(Object key, Text values, Context context) throws InterruptedException {
        String[] stock_volumes = values.toString().split(",");

        if (!stock_volumes[7].equals("stock_volume")) {
            max_stock_date = stock_volumes[2];
            min_stock_date = stock_volumes[2];
            max_stock_volume = Integer.parseInt(stock_volumes[7]);
            min_stock_volume = Integer.parseInt(stock_volumes[7]);
            max_stock_price_adj_close = Double.parseDouble(stock_volumes[8]);

            if (max_stock_date == null || min_stock_date == null || min_stock_volume == null || max_stock_volume == null || max_stock_price_adj_close == null) {
                return;
            }

            try {
                wo.setMax_stock_volume(max_stock_volume);
                wo.setMin_stock_volume(min_stock_volume);
                wo.setMax_stock_price_adj_close(max_stock_price_adj_close);
                wo.setMax_stock_date(max_stock_date);
                wo.setMin_stock_date(min_stock_date);
                context.write(new Text(stock_volumes[1]), wo);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}