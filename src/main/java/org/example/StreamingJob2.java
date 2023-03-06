package org.example;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.fx.CancalBinlogRow;
import org.example.fx.CancalFormatCkSink;
import org.example.fx.ClickhouseSinkBuilder;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class StreamingJob2 {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        System.out.println("Window WordCount");



        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.168.4.184:9092")
                .setTopics("test")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.timestamp(Timestamp.valueOf("2023-03-03 09:00:17").getTime()))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();



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



        kafkaSource.addSink(new CancalFormatCkSink()
                .withHost("192.168.15.111", "8123")
                .withAuth("default", "default", "")
        );

        env.execute("Window WordCount");
    }


}



