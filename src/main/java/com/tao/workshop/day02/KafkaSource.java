package com.tao.workshop.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        //指定kafka的broker地址
        properties.setProperty("bootstrap.servers","bigdata1:9092");
        //指定组ID
        properties.setProperty("group.id","taosm10");
        //如果没有提交偏移量，就从
        properties.setProperty("auto.offset.reset","earliest");
        //properties.setProperty("enable.auto.commit","false");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("wc20", new SimpleStringSchema(), properties);
        DataStream<String> lines = env.addSource(kafkaSource);
        lines.print();
        env.execute();
    }
}
