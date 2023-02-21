package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/15 20:56
 * @Version: 1.0
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {

            outK.set(word);
            context.write(outK, outV);
        }
    }
}
