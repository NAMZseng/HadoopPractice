package cn.nam.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * 自定义如何对key分组，比较两个DateTemperaturePair对象
 * 将年月相同的键分组到同一个Reducer.reduce()方法调用
 *
 * @author Nanrong Zeng
 * @version 1.0
 */

public class DateTemperatureGroupingComparator extends WritableComparator {

    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair1 = (DateTemperaturePair) a;
        DateTemperaturePair pair2 = (DateTemperaturePair) b;

        return pair1.getYearMonth().compareTo(pair2.getYearMonth());

    }
}
