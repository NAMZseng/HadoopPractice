package cn.nam.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Nanrong Zeng
 * @version 1.0
 */
public class WordCountJob {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        // 设置跨平台提交
        conf.set("mapreduce.app-submission.cross-platform", "true");
        // 本地直接调用远程环境的HADOOP, 需上传执行文件
        conf.set("mapred.jar", "F:\\ATech\\Codes\\HadoopPractice\\target\\HadoopPractice-1.0-SNAPSHOT.jar");

        Job job = null;

        try {
            job = Job.getInstance(conf, "myWordCount");
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 设置job执行主方法
        job.setJarByClass(WordCountJob.class);
        // 设置Map方法名
        job.setMapperClass(WordCountMap.class);
        // 设置Reduce方法名
        job.setReducerClass(WordCountReduce.class);
        // 设置map输出的key类型
        job.setOutputKeyClass(Text.class);
        // 设置map输出的value类型
        job.setMapOutputValueClass(IntWritable.class);

        // 设置读取输入文件的位置（为HDFS中的文件位置）
        try {
            FileInputFormat.addInputPath(job, new Path("/test/wc/input/"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 设置处理任务后数据存放的位置
        // 注：输出文件夹不可预先创建，只能有job自动创建
        Path outpath = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            outpath = new Path("/test/wc/output");
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job, outpath);

        // 提交任务等待结束
        boolean flag = false;
        try {
            flag = job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (flag == true) {
            System.out.println("JOB finished successfully!");
        } else {
            System.out.println("JOB failed!");
        }


    }
}
