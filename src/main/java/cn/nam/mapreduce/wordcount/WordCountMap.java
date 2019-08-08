package cn.nam.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Nanrong Zeng
 * @version 1.0
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] str = value.toString().split(" ");

        for (String s : str) {
            //                k2        v2
            context.write(new Text(s), one);
        }

    }
}
