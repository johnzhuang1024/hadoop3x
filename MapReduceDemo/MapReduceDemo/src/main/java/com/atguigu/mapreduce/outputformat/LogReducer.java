package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 9:51
 */
public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 防止有相同的数据,迭代写出
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
