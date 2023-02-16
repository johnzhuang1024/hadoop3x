package com.atguigu.mapreduce.writable;

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
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        //1 获取一行数据,转成字符串
        String line = value.toString();

        //2 切割数据
        String[] split = line.split("\t");

        //3 抓取我们需要的数据:手机号,上行流量,下行流量
        String phone = split[1];
        String upFlowValue = split[split.length - 3];
        String downFlowValue = split[split.length - 2];

        //4 封装outK outV
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upFlowValue));
        outV.setDownFlow(Long.parseLong(downFlowValue));
        outV.setSumFlow();

        //5 写出outK outV
        context.write(outK, outV);
    }
}
