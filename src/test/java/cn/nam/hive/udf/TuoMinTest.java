package cn.nam.hive.udf;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Nanrong Zeng
 * @version 1.0
 */
public class TuoMinTest {
    @Test
    public void tuoMinTest(){

        String s = "赵六";
        String str = s.toString();
        System.out.println(str.length());
        if (str.length() <= 3) {
            System.out.println(str.substring(0, str.length()-1) + "***");
        } else {
            System.out.println(str.substring(0, 3) + "***");
        }
    }
}