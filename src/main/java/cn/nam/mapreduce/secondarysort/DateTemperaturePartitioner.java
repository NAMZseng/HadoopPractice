package cn.nam.mapreduce.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器，控制相同年月的键被分到同一Reducer规约器处理
 *
 * @author Nanrong Zeng
 * @version 1.0
 */
public class DateTemperaturePartitioner
        extends Partitioner<DateTemperaturePair, Text> {

    @Override
    public int getPartition(DateTemperaturePair key, Text value, int numPartitions) {
        // 确保分区号非负
        return Math.abs(key.getYearMonth().hashCode() % numPartitions);
    }
}
