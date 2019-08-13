package cn.nam.mapreduce.wordcount;

import cn.nam.hdfs.HdfsConnectUtil;
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

        Configuration conf = HdfsConnectUtil.getHdfsConn();
        // 设置跨平台提交任务
        conf.set("mapreduce.app-submission.cross-platform", "true");
        // 设置任务jar包，需提前使用maven命令（mvn assembly:assembly）生成
        conf.set("mapred.jar",
                "F:\\ATech\\Codes\\HadoopPractice\\target\\HadoopPractice-1.0-SNAPSHOT.jar");

        FileSystem fs = HdfsConnectUtil.getFileSystem();
        Path inputPath = new Path("/tmp/znr/wc/input/wcinput.txt");
        try {
            fs.mkdirs(inputPath);
            fs.copyFromLocalFile(new Path("O:\\wcinput.txt"), inputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Path outpath = new Path("/tmp/znr/wc/output");
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

        try {
            // 设置读取输入文件的位置（为HDFS中的文件位置）
            FileInputFormat.addInputPath(job, inputPath);

            // 设置处理任务后数据存放的位置
            // 注：输出文件夹不可预先创建，只能有job自动创建
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
