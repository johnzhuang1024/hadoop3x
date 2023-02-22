package com.atguigu.mapreduce.partionerandwritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 9:51
 */
public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {


    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString();

        String pre3phone = phone.substring(0, 3);

        int partition;
        if ("136".equals(pre3phone)) {
            partition = 0;
        } else if ("137".equals(pre3phone)) {
            partition = 1;
        } else if ("138".equals(pre3phone)) {
            partition = 2;
        } else if ("139".equals(pre3phone)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;
    }
}
