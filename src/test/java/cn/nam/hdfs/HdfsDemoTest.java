package cn.nam.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Nanrong Zeng
 * @version 1.0
 */
public class HdfsDemoTest {
    private FileSystem fs = HdfsConnectUtil.getFileSystem();
    ;
    private Configuration conf = HdfsConnectUtil.getHdfsConn();

    /**
     * 测试连接
     */
    @Test
    public void hdfsConnectTest() {
        System.out.println(fs.toString());
        // DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-17092761_1, ugi=root (auth:SIMPLE)]]
    }

    /**
     * 从hdfs下载文件到本地
     */
    @Test
    public void copyToLocalTest() {
        try {
            fs.copyToLocalFile(new Path("/tmp/znr/olddata"), new Path("O:\\"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将本地文件上传到hdfs
     */
    @Test
    public void copyFromLocalTest() {
        // 自定义文件备份数，默认为3
        conf.set("dfs.replication", "2");
        try {
            FileSystem putFs = FileSystem.get(new URI("hdfs://node01:8020"), conf, "znr");
            putFs.copyFromLocalFile(new Path(""),
                    new Path("/tmp/znr/spark_data"));
//            使用默认replication=3上传
//            fs.copyFromLocalFile(new Path("O:\\apitest"), new Path("/tmp/znr"));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建目录, 并设置权限
     */
    @Test
    public void mkDirTest() {
        try {
            fs.mkdirs(new Path("/tmp/znr/spark_data"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件/文件夹
     */
    @Test
    public void deleteTest() {
        try {
            fs.delete(new Path("/tmp/znr/newdata"), false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 更改文件的名称
     */
    @Test
    public void renameTest() {
        try {
            fs.rename(new Path("/tmp/znr/lesson2"), new Path("/tmp/znr/hdfs_data"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 判断是文件还是文件夹
     */
    @Test
    public void listFileorDirectoryTest() {
        try {
            FileStatus[] listStatus = fs.listStatus(new Path("/tmp/znr/"));
            for (FileStatus fileStatus : listStatus){
                System.out.println("FileName: " + fileStatus.getPath().getName());
                System.out.println("Owner: " + fileStatus.getOwner());

                // 判断是文件还是文件夹
                String type = fileStatus.isDirectory() ? "d" : "-";
                System.out.println("Permission: " + type + fileStatus.getPermission());
                System.out.println("------------------------------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定路径的文件详情
     */
    @Test
    public void listFilesTest() {
        try {
            // 设为true则表示recursive，可显示子文件夹内容
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/tmp/znr/"), false);

            while (listFiles.hasNext()) {
                LocatedFileStatus status = listFiles.next();

                System.out.println("FileName: " + status.getPath().getName());
                System.out.println("Owner: " + status.getOwner());
                System.out.println("Group: " + status.getGroup());
                System.out.println("Permission: " + status.getPermission());

                // 时间格式转换 long--> date
                Date date = new Date(status.getModificationTime());
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                System.out.println("Last Modified: " + sdf.format(date));
                System.out.println("Size: " + status.getLen() + "B");
                System.out.println("Replication: " + status.getReplication());

                // 每个文件可能被划分为多个Block，获取所有Block的信息
                BlockLocation[] locations = status.getBlockLocations();
                int i = 0;
                for (BlockLocation blockLocation : locations) {
                    // 每个Block可能存在多个备份，获取每个备份所在的主机信息
                    String[] hosts = blockLocation.getHosts();
                    System.out.println("---------Block_" + i++ + "---------");
                    for (String host : hosts) {
                        System.out.println("HostName: " + host);
                    }

                }
                System.out.println("------------------------------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过IO流的方式上传文件
     */
    @Test
    public void putFileIOTest()  {
        FileInputStream fis = null;
        FSDataOutputStream fos = null;
        try {
            fis = new FileInputStream(new File("O:\\iotest"));

            // TODO 测试当文件不存在时，是否会创建新文件
            fos = fs.create(new Path("/tmp/znr/newdata"));

            IOUtils.copyBytes(fis, fos, conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }


}