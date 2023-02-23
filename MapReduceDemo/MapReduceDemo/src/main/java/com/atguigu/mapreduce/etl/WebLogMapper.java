package com.atguigu.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 14:52
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // 获取1行
        // 163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
        String line = value.toString();

        // 解析日志
        boolean result = parseLog(line, context);

        // 判断日志是否合法
        if(!result){
            return;
        }else {
            context.write(value, NullWritable.get());
        }
    }

    private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {

        // 截取
        String[] fields = line.split(" ");

        // 根据业务需求判断日志长度是否大于11
        if(fields.length > 11){
            return true;
        }else {
            return false;
        }
    }
}
