package com.atguigu.mapreduce.partionerandwritableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/16 15:57
 * @Version: 1.0
 */

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 9:51
 */
//1 implements继承WritableComparable接口
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow; // 上行流量
    private long downFlow; // 下行流量
    private long sumFlow; // 总流量

    //2 提供无参构造
    public FlowBean() {
    }

    //3 提供三个参数的getter和setter方法
    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    //4 实现序列化和反序列化方法,注意顺序一定要保持一致
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    //5 重写ToString
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {

        // 总流量的倒序排序
        if (this.sumFlow > o.sumFlow) {
            return -1;
        } else if (this.sumFlow < o.sumFlow) {
            return 1;
        } else {
            // 上行流量的正序排序
            if (this.upFlow > o.upFlow) {
                return 1;
            } else if (this.upFlow < o.upFlow) {
                return -1;
            } else {
                // 下行流量的正序排序
                if (this.downFlow > o.downFlow) {
                    return 1;
                } else if (this.downFlow < o.downFlow) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
    }
}
