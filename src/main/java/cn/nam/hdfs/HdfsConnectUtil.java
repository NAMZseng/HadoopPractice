package cn.nam.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hadoop dfs 连接工具类
 *
 * @author Nanrong Zeng
 * @version 1.0
 */
public class HdfsConnectUtil {
    private Configuration configuration = null;
    private FileSystem fileSystem = null;

    /**
     * 静态内部类，实现单例模式，同时保证线程安全
     */
    private static class SingletonHolder {
        private static final HdfsConnectUtil INSTANCE = new HdfsConnectUtil();
    }

    private HdfsConnectUtil() {
        configuration = new Configuration();
        // 私有云
        configuration.set("fs.defaultFS", "hdfs://node01:8020/");

        try {
            // 设置操作用户
            // 用户需要在hdfs的/tmp目录下新建一个个人目录，如/tmp/znr
            fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration, "znr");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取hdfs连接Configuration单例
     *
     * @return Configuration
     */
    public static Configuration getHdfsConn() {
        return SingletonHolder.INSTANCE.configuration;
    }

    /**
     * 获取hdfs文件FileSystem单例
     *
     * @return FileSystem
     */
    public static FileSystem getFileSystem() {
        return SingletonHolder.INSTANCE.fileSystem;
    }

}
