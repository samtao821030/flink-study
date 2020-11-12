package com.tao.workshop.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class LambdaStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            Arrays.stream(line.split(" ")).forEach(w -> {
                out.collect(Tuple2.of(w, 1));
            });
        }).returns(Types.TUPLE(Types.STRING,Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.keyBy(0).sum(1);
        sum.print();
        env.execute("Lambda表达式WordCount");
    }
}
