package org.example;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.example.fx.CancalBinlogRow;
import org.example.fx.CancalFormatCkSink;
import org.example.fx.ClickhouseSinkBuilder;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class StreamingJob2 {

    public static void main(String[] args) throws Exception {

        // chaeckpoint 存储地址
        final String s3checkpoint = "s3://fmflink-checkpoint/checkpoint";

        // 1：获取flink任务执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

//        // 2.1 开启 Checkpoint,每隔 10 秒钟做一次 CK
//        env.enableCheckpointing(10000L);
//        //2.2 指定 CK 的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次 CK 数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从 CK 自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 20000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend(s3checkpoint));


        //创建 KafkaSource 读取kafka消息，此时保证消费者组内的消息不重复消费
        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("10.168.4.184:9092")
//                .setTopics("test")
//                .setGroupId("my-group")
                .setBootstrapServers("pbs-kafka-6wylvu-0.kafka.queue.inneryiche.com:9092,pbs-kafka-6wylvu-1.kafka.queue.inneryiche.com:9092,pbs-kafka-6wylvu-2.kafka.queue.inneryiche.com:9092")
                .setTopics("mysql_to_ck")
                .setGroupId("my-group")

                .setStartingOffsets(OffsetsInitializer.latest())

//                .setProperty("security.protocol","SASL_PLAINTEXT")
//                .setProperty("sasl.mechanism","SCRAM-SHA-256")
//                .setProperty("sasl.jaas.config",
//                        "org.apache.kafka.common.security.plain.PlainLoginModule required username='sunshiyun' password='sunshiyunfuhznp';")
                .setProperty("security.protocol","SASL_PLAINTEXT")
                .setProperty("sasl.mechanism","SCRAM-SHA-256")
                .setProperty("sasl.jaas.config",
//                        "org.apache.kafka.common.security.plain.PlainLoginModule required username='sunshiyun' password='sunshiyunfuhznp';")
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sunshiyun\" password=\"sunshiyunfuhznp\";")
//                .setStartingOffsets(OffsetsInitializer.timestamp(Timestamp.valueOf("2023-03-03 09:00:17").getTime()))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        System.out.println("111111111111111111");


        ObjectMapper jsonMapper = new ObjectMapper();

        SingleOutputStreamOperator<List<CancalBinlogRow>> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source")
                .map(jsonLine -> jsonMapper.readValue(jsonLine, CancalBinlogRow.class))
                .keyBy(value -> value.getDatabase())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<CancalBinlogRow, List<CancalBinlogRow>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<CancalBinlogRow, List<CancalBinlogRow>, String, TimeWindow>.Context context, Iterable<CancalBinlogRow> input, Collector<List<CancalBinlogRow>> out) throws Exception {
                        List<CancalBinlogRow> list = new ArrayList<>();
                        input.forEach(single -> list.add(single));
                        if (list.size() > 0) {
                            out.collect(list);
                        }

                    }
                });



//        kafkaSource.addSink(new CancalFormatCkSink()
//                .withHost("192.168.15.111", "8123")
//                .withAuth("default", "default", "")
//        );
        kafkaSource.addSink(new CancalFormatCkSink()
                .withHost("clickhouse-yccb-analysis.inneryiche.com", "8123")
                .withAuth("yccb_analysis", "yccb_analysis_user_rw", "jhdnmshTBgs")
        );
        env.execute("Sync Mysql to ClickHouse");
    }


}



