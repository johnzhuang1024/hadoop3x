package com.atguigu.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/22 10:43
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        // 创建两个对象，一个专门存order的数据，一个专门存pd的数据。为了后期直接循环order，并用pd直接赋值。
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        // 循环判断是order还是pd，并且进行不一样的操作。
        for (TableBean value : values) {
            if ("order".contains(value.getPname())) {
                // 由于hadoop中的values传进来的是地址，如果直接用地址给前的ArrayList对象赋值，会导致覆盖！
                // 因此重新new一个全新的TableBean对象，然后把之前的数据copy过来，再把新的TableBean对象add进ArrayList中即可！
                TableBean tmpOrderBeans = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpOrderBeans, value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                orderBeans.add(tmpOrderBeans);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // 循环遍历每一个orderBean，替换之前的空值Pname。
        for (TableBean orderBean : orderBeans) {
            // 把pd中的Pname赋值给order
            orderBean.setPname(pdBean.getPname());

            // 写出
            context.write(orderBean, NullWritable.get());
        }
    }
}
