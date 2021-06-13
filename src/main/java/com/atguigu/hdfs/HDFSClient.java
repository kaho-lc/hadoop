package com.atguigu.hdfs;

/**
 * @author lc
 * @create 2021-04-19-12:52
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码的常用套路
 * 1.获取一个客户端对象
 * 2.执行相关的操作命令
 * 3.关闭资源
 * HDFS客户端操作 ， Zookeeper客户端操作
 */
public class HDFSClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群，nn地址
        URI uri = new URI("hdfs://hadoop102:8020");

        //配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication" , "2");

        //用户
        String user = "atguigu";
        //获取到客户端对象
        fs = FileSystem.get(uri, configuration , user);

    }

    @After
    public void close() throws IOException {
        //关闭资源
        fs.close();
    }


    @Ignore
    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {

        //执行相关命令操作：创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));

    }

    //上传一个文件

    /**
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml=>在项目资源目录下的配置文件=>代码里面的配置优先级最高
     * @throws IOException
     */
    @Ignore
    @Test
    public void testPut() throws IOException {
    // 参数解读：参数1：表示删除元数据 ， 参数二：表示是否允许覆盖 ， 参数三：原数据路径 ， 参数四：目的路径
    fs.copyFromLocalFile(false, true, new Path("D:\\data\\sunwukong.txt") , new Path("/xiyou/huaguoshan1"));
    }

    //文件下载
    //参数解读：参数一：原文件是否删除 ， 参数二：表示原文件的路径 ， 参数三：目标地址路径 ， 参数四：
    @Ignore
    @Test
    public void testGet() throws IOException {
    fs.copyToLocalFile(true, new Path("/xiyou/huaguoshan1"), new Path("D:\\") , true);
    }

    //删除
    @Test
    @Ignore
    public void testRm() throws IOException {
        //删除文件
        //参数解读：参数一：要删除的路径 ， 参数二：是否递归删除
        fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz") , false);

        //删除空目录
        fs.delete(new Path("/xiyou") , false);

        //删除非空目录
        fs.delete(new Path("/jinguo"),true);
    }


    //文件的更名和移动
    @Ignore
    @Test
    public void testMv() throws IOException {
        //对文件名称的修改
        //参数解读：参数一：原文件路径 ， 参数二：目标文件的路径
        fs.rename(new Path("/wcinput/word.txt") , new Path("/wcinput/ss.txt"));

        //文件的移动和更名
        fs.rename(new Path("/wcinput/ss.txt") , new Path("/cls.txt"));

        //目录的更名
        fs.rename(new Path("/wcinput") , new Path("/output3"));
    }


    //查看文件名称、权限、长度、块信息
    @Ignore
    @Test
    public void fileDetail() throws IOException {

        //获取所有的文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        // 遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("===============" + fileStatus.getPath() + "===============");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }


    //判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status: listStatus ) {
            if (status.isFile()) {
                System.out.println("文件：" + status.getPath().getName());
            }else {
                System.out.println("文件夹：" + status.getPath().getName());
            }
        }
    }
}
