package com.atguigu.mapreduce.outputformat;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
// 长的包
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // 自定义一个返回对象RecordWriter，重新写个类是为了更好地管理代码
        LogRecordWriter recordWriter = new LogRecordWriter(job);
        return recordWriter;
    }
}
