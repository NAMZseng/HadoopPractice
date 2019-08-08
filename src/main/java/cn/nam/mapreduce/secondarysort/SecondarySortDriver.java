package cn.nam.mapreduce.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


/**
 * @author Nanrong Zeng
 * @version 1.0
 */
public class SecondarySortDriver {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapred.jar",
                "F:\\ATech\\Codes\\HadoopPractice\\target\\HadoopPractice-1.0-SNAPSHOT.jar");

        try {
            Job job = Job.getInstance(conf, "mySecondatySort");
            job.setJarByClass(SecondarySortDriver.class);
            job.setMapperClass(SecondarySortMapper.class);

            // 设置map的输入输出类型
            job.setOutputKeyClass(DateTemperaturePair.class);
            job.setMapOutputValueClass(FloatWritable.class);

            job.setReducerClass(SecondarySortReducer.class);
            job.setPartitionerClass(DateTemperaturePartitioner.class);
            job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);


            FileInputFormat.addInputPath(job, new Path("/test/secondarySort/input/"));
            FileSystem fs = FileSystem.get(conf);
            Path outpath = new Path("/test/secondarySort/output");
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean flag = false;
            flag = job.waitForCompletion(true);
            if (flag == true) {
                System.out.println("JOB finished successfully!");
            } else {
                System.out.println("JOB failed!");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
