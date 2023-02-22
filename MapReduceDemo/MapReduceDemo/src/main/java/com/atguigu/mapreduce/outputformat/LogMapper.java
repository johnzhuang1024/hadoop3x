package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 9:51
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 不做任何处理，直接输出一行log数据
        context.write(value, NullWritable.get());
    }
}
