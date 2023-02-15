package com.atguigu.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/15 16:10
 * @Version: 1.0
 */
public class HdfsClient {

    private FileSystem fs;

    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();

        // 优先级测试
        // 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的自定义配置（xxx-site.xml） >（4）服务器的默认配置（xxx-default.xml）
        configuration.set("dfs.replication", "2");

        // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
        fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "atguigu");
    }

    public void close() throws IOException {
        // 关闭资源
        fs.close();
    }


    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {

        // 初始化配置
        init();

        // 1 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));

        // 关闭资源
        close();
    }


    @Test
    public void test_copyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {
        // 初始化配置
        init();

        // 2 上传文件
        fs.copyFromLocalFile(new Path("d:/sunwukong.txt"), new Path("/xiyou/huaguoshan"));

        // 关闭资源
        close();
    }

    @Test
    public void test_copyToLocalFile() throws IOException, URISyntaxException, InterruptedException {
        // 初始化配置
        init();

        // 3 下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("d:/sunwukong2.txt"), true);

        // 关闭资源
        close();
    }

    @Test
    public void test_rename() throws IOException, URISyntaxException, InterruptedException {
        // 初始化配置
        init();

        // 4 修改文件名称
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("/xiyou/huaguoshan/meihouwang.txt"));

        // 关闭资源
        close();
    }

    @Test
    public void test_delete() throws IOException, URISyntaxException, InterruptedException {
        // 初始化配置
        init();

        // 5 执行删除
        // b参数 是否强制删除
        fs.delete(new Path("/xiyou"), true);

        // 关闭资源
        close();
    }


    @Test
    public void test_listFiles() throws IOException, URISyntaxException, InterruptedException {
        // 初始化配置
        init();

        // 6 获取文件详情
        // recursive 递归
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        // 关闭资源
        close();
    }

    @Test
    public void test_listStatus() throws IOException, URISyntaxException, InterruptedException {
        // 初始化配置
        init();

        // 6 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 关闭资源
        close();
    }
}
