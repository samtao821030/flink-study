package com.tao.workshop.day02.sources;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


public class TextFileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.readTextFile(args[0]);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flats = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            Arrays.stream(line.split(" ")).forEach(w -> {
                out.collect(Tuple2.of(w, 1));
            });
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> keyPairs = flats.keyBy(0).sum(1);
        keyPairs.print();
        env.execute("读取文本文件");
    }
}
