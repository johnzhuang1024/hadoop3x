package com.atguigu.mapreduce.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/15 20:57
 * @Version: 1.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int sum;
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 1 累加
        sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 输出
        outV.set(sum);
        context.write(key, outV);
    }
}
