package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        
        String phone = text.toString();
        String pre3Phone = phone.substring(0, 3);

        int partition;
        if("136".equals(pre3Phone)){
            partition = 0;
        }else if("137".equals(pre3Phone)){
            partition = 1;
        }else if("138".equals(pre3Phone)){
            partition = 2;
        }else if("139".equals(pre3Phone)){
            partition = 3;
        }else {
            partition = 4;
        }

        return partition;
    }
}
