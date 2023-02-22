package com.atguigu.mapreduce.reduceJoin;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 10:11
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        // 获取文件名称
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();

        //判断是哪个文件,然后针对文件进行不同的操作
        if (filename.contains("order")) {  //订单表的处理
            String[] split = line.split("\t");
            //封装outK
            outK.set(split[1]);
            //封装outV
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else {                             //商品表的处理
            String[] split = line.split("\t");
            //封装outK
            outK.set(split[0]);
            //封装outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }


        //    @Override
//    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
//        // 获取一行
//        String line = value.toString();
//
//        // 判断是哪个文件，针对不同文件进行不同操作
//        if(name.contains("order")){// 订单表处理
//            String[] split = line.split("\t");
//            // 封裝outK
//            outK.set(split[1]);
//            // 封裝outV
//            outV.setId(split[0]);
//            outV.setPid(split[1]);
//            outV.setAmount(Integer.parseInt(split[2]));
//            outV.setPname("");
//            outV.setFlag("order");
//        } else{// 商品表处理
//            String[] split = line.split("\t");
//            // 封裝outK
//            outK.set(split[0]);
//            // 封裝outV
//            outV.setId("");
//            outV.setPid(split[0]);
//            outV.setAmount(0);
//            outV.setPname(split[1]);
//            outV.setFlag("pd");
//        }

        // 写出outK和outV
        context.write(outK, outV);
    }
}
