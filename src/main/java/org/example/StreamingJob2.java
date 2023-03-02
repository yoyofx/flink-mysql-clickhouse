package org.example;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsonschema.JsonSerializableSchema;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.fx.CancalBinlogRow;

import java.sql.Timestamp;
import java.util.List;

public class StreamingJob2 {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("Window WordCount");

//        String str = "{\"data\":[{\"id\":\"1\",\"job_id\":\"1\",\"glue_type\":\"1\",\"glue_source\":\"1\",\"glue_remark\":\"1\",\"add_time\":\"2023-03-01 13:45:15\",\"update_time\":\"2023-03-01 13:45:21\"}],\"database\":\"xxl-job\",\"es\":1677649523000,\"gtid\":\"\",\"id\":3,\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"job_id\":\"int(11)\",\"glue_type\":\"varchar(50)\",\"glue_source\":\"mediumtext\",\"glue_remark\":\"varchar(128)\",\"add_time\":\"datetime\",\"update_time\":\"datetime\"},\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":4,\"job_id\":4,\"glue_type\":12,\"glue_source\":2005,\"glue_remark\":12,\"add_time\":93,\"update_time\":93},\"table\":\"xxl_job_logglue\",\"ts\":1677649523714,\"type\":\"INSERT\"}";
//
//        ObjectMapper jsonMapper = new ObjectMapper();
//        CancalBinlogRow rows = jsonMapper.readValue(str,CancalBinlogRow.class);
//        System.out.println(rows);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.168.4.184:9092")
                .setTopics("test")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.timestamp(Timestamp.valueOf("2023-03-01 00:00:00").getTime()))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


//        DataStream<Tuple2<String, Integer>> dataStream = env
//                .fromSource(source, WatermarkStrategy.noWatermarks(),"kafka source")
//                //.socketTextStream("localhost", 9999)
//                .flatMap(new StreamingJob2.Splitter())
//                .keyBy(value -> value.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(1);

//		DataStream<String> dataStream = env
//				.socketTextStream("localhost", 9999);
        ObjectMapper jsonMapper = new ObjectMapper();

        env.fromSource(source, WatermarkStrategy.noWatermarks(),"kafka source")
                .map(jsonLine -> jsonMapper.readValue(jsonLine, CancalBinlogRow.class))
                .keyBy(value -> value.getDatabase())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .process(new ProcessWindowFunction<CancalBinlogRow, List<CancalBinlogRow>,String, TimeWindow>(){
                    @Override
                    public void process(String key, ProcessWindowFunction<CancalBinlogRow, List<CancalBinlogRow> , String, TimeWindow>.Context context, Iterable<CancalBinlogRow> input, Collector<List<CancalBinlogRow>> out) throws Exception {
                        List<CancalBinlogRow> list = Lists.newArrayList(input);
                        if (list.size() > 0) {
                            out.collect(list);
                        }
                    }
                })
                .print();


        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}



