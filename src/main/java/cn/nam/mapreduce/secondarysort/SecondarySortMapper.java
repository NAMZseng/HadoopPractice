package cn.nam.mapreduce.secondarysort;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Nanrong Zeng
 * @version 1.0
 */
public class SecondarySortMapper
        extends Mapper<LongWritable, Text, DateTemperaturePair, FloatWritable> {

    private final DateTemperaturePair pair = new DateTemperaturePair();
    private final FloatWritable temperature = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");
        int len = 4;
        // 防止输入文件的格式不符合要求，造成数组越界异常
        if (tokens.length < len){
            return;
        }

        // YYYY = tokens[0]
        // MM = tokens[1]
        // DD = tokens[2]
        // temperature = tokens[3]

        pair.setYearonth(tokens[0] + "-" + tokens[1]);
        pair.setDay(tokens[2]);
        float tem = Float.parseFloat(tokens[3]);
        pair.setTemperature(tem);
        temperature.set(tem);

        context.write(pair, temperature);
    }
}
