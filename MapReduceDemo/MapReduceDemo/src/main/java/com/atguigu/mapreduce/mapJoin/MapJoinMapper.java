package com.atguigu.mapreduce.mapJoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;


/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 11:28
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();

    //任务开始前将pd数据缓存进pdMap
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException {
        // 通过缓存文件，得到第一个小表pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        // 获取文件系统对象，并开流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fileSystem.open(path);

        // 通过包装流转换为reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(open, "UTF-8"));

        String line;
        while (StringUtils.isNoneEmpty(line = reader.readLine())) {
            //切割一行
            //01	小米
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }
        //关流
        IOUtils.closeStreams(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // 读取大表数据
        // 1001	01	1
        String s = value.toString();
        String[] split = s.split("\t");

        // 通过大表每行数据的pid,去pdMap里面取出pname
        String pname = pdMap.get(split[1]);

        // 获取订单id和订单数量
        outK.set(split[0] + "\t" + pname + "\t" + split[2]);

        // 写出数据
        context.write(outK, NullWritable.get());
    }
}
