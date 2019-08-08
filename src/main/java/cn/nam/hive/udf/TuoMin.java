package cn.nam.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 对关键字符脱敏，仅显示前3个字符，其余部分用***表示
 *
 * @author Nanrong Zeng
 * @version 1.0
 */
public class TuoMin extends UDF {

    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        String str = s.toString();
        if (str.length() <= 3) {
            str = str.substring(0, str.length()-1) + "***";
        } else {
            str = str.substring(0, 3) + "***";
        }
        return new Text(str);
    }
}
