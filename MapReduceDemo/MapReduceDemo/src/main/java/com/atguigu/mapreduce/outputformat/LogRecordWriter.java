package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 9:51
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());

            atguiguOut = fileSystem.create(new Path("D:\\environment\\DataFile\\output\\OutputFormatOutput\\atguigu"));
            otherOut = fileSystem.create(new Path("D:\\environment\\DataFile\\output\\OutputFormatOutput\\other"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();

        // 工具一行log数据是否包含atguigu，判断两条输出流的输出内容
        if (log.contains("atguigu")) {
            atguiguOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 关流
        IOUtils.closeStreams(atguiguOut, otherOut);
    }
}
