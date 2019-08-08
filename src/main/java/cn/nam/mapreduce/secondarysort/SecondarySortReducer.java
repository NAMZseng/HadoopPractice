package cn.nam.mapreduce.secondarysort;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Nanrong Zeng
 * @version 1.0
 */
public class SecondarySortReducer
        extends Reducer<DateTemperaturePair, FloatWritable, Text, Text> {

    @Override
    protected void reduce(DateTemperaturePair key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder stringBuilder = new StringBuilder();
        for (FloatWritable value : values) {
                stringBuilder.append(value.toString());
                stringBuilder.append(",");
        }
        Text text = new Text(stringBuilder.toString());
        context.write(key.getYearMonth(), text);
    }
}
