package cn.nam.mapreduce.secondarysort;

/**
 * 文件每行输入格式 year, month, day, temperature
 * 自定义组合key-valued对,形式为((year,month, temperature), temperature)
 *
 * @author Nanrong Zeng
 * @version 1.0
 */

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class DateTemperaturePair implements WritableComparable {

    private Text yearMonth = null;
    private Text day = null;
    private FloatWritable temperature = null;

    public DateTemperaturePair() {
        yearMonth = new Text();
        day = new Text();
        temperature = new FloatWritable();
    }

    public void setYearonth(String yearMonth) {
        this.yearMonth.set(yearMonth);
    }

    public void setDay(String day) {
        this.day.set(day);
    }

    public void setTemperature(Float temperature) {
        this.temperature.set(temperature);
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public FloatWritable getTemperature() {
        return temperature;
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        DateTemperaturePair that = (DateTemperaturePair) obj;

        // 当温度、年月分均相等时，才相等
        // 因为温度是次要比较因素，所以在判断相等时，先比较温度
        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
            return false;
        }
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        // StringBuilder 使用处理大量缓存,在单线程中效率最快
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DateTemperturePair"
                + "{yearMonth:" + yearMonth
                + ", day:" + day
                + ", temperature:" + temperature
                + "}");
        return stringBuilder.toString();
    }

    @Override
    public int compareTo(Object o) {
        DateTemperaturePair that = (DateTemperaturePair) o;
        int result = this.yearMonth.compareTo(that.getYearMonth());
        if (0 == result) {
            result = this.temperature.compareTo(that.getTemperature());
        }

        // 升序
        return result;
        // 降序
        // return -1 * result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //通过write方法写入序列化的数据流
        yearMonth.write(out);
        day.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //通过readFields方法从序列化的数据流中读出进行赋值
        yearMonth.readFields(in);
        day.readFields(in);
        temperature.readFields(in);
    }
}
