package com.atguigu.mapreduce.partionerandwritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/16 16:13
 * @Version: 1.0
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();

        // 切割
        String[] split = line.split("\t");

        // 封装
        outV.set(split[0]);
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();

        // 写出
        context.write(outK, outV);
    }
}
