package com.tao.workshop.day02.sources;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SourceDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6666);
        DataStreamSource<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        SingleOutputStreamOperator<Integer> filters = nums.filter((Integer num) -> {
            if (num % 2 == 0) {
                return true;
            } else {
                return false;
            }
        }).returns(Types.INT);
        //filters.setParallelism(1);
        filters.print();
        env.execute("过滤流");
    }
}
