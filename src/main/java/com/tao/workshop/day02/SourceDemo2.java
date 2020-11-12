package com.tao.workshop.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Arrays;

public class SourceDemo2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);

    }
}
