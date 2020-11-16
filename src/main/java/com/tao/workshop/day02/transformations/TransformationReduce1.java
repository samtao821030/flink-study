package com.tao.workshop.day02.transformations;

import com.tao.workshop.day02.transformations.bean.Population;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 　　　　　　　 ┏┓       ┏┓+ +
 * 　　　　　　　┏┛┻━━━━━━━┛┻┓ + +
 * 　　　　　　　┃　　　　　　 ┃
 * 　　　　　　　┃　　　━　　　┃ ++ + + +
 * 　　　　　　 █████━█████  ┃+
 * 　　　　　　　┃　　　　　　 ┃ +
 * 　　　　　　　┃　　　┻　　　┃
 * 　　　　　　　┃　　　　　　 ┃ + +
 * 　　　　　　　┗━━┓　　　 ┏━┛
 * ┃　　  ┃
 * 　　　　　　　　　┃　　  ┃ + + + +
 * 　　　　　　　　　┃　　　┃　Code is far away from     bug with the animal protecting
 * 　　　　　　　　　┃　　　┃ + 　　　　         神兽保佑,代码无bug
 * 　　　　　　　　　┃　　　┃
 * 　　　　　　　　　┃　　　┃　　+
 * 　　　　　　　　　┃　 　 ┗━━━┓ + +
 * 　　　　　　　　　┃ 　　　　　┣┓
 * 　　　　　　　　　┃ 　　　　　┏┛
 * 　　　　　　　　　┗┓┓┏━━━┳┓┏┛ + + + +
 * 　　　　　　　　　 ┃┫┫　 ┃┫┫
 * 　　　　　　　　　 ┗┻┛　 ┗┻┛+ + + +
 */
public class TransformationReduce1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = env.fromCollection(
                Arrays.asList("辽宁,沈阳,1000", "山东,青岛,2000", "山东,青岛,1000", "山东,烟台,1000"));
        SingleOutputStreamOperator<Population> mapLines = words.map((String line) -> {
            String[] lineArray = line.split(",");
            return new Population(lineArray[0], lineArray[1], Long.parseLong(lineArray[2]));
        }).returns(Population.class);
        SingleOutputStreamOperator<Population> reduceLines = mapLines.keyBy("province", "city").reduce((Population p1, Population p2) -> {
            return new Population(p1.province, p1.city, (p1.num + p2.num));
        }).returns(Population.class);
        reduceLines.print();
        env.execute("TransformationKeyBy3");
    }
}
