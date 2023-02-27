package org.example.fx;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.StreamData;

import java.lang.reflect.Field;

public class ClickhouseSinkBuilder<T> {

    private String ckHost = "";
    private String ckPort = "";
    private String user = "";
    private String password = "";
    private String dbName = "";

    public ClickhouseSinkBuilder<T> withHost(String ckHost,String ckPort) {
        this.ckHost = ckHost;
        this.ckPort = ckPort;
        return this;
    }

    public ClickhouseSinkBuilder<T> withAuth(String db,String userName,String passWord) {
        this.dbName = db;
        this.user = userName;
        this.password = passWord;
        return this;
    }

    public SinkFunction<T> Build(Class<T> dataType)  {
        Field[] fields = dataType.getDeclaredFields();
        Table attr = dataType.getDeclaredAnnotation(Table.class);
        String tableName = attr.value();
        String Sql = SQLHelper.getSqlInset(tableName,fields);
        System.out.println(Sql);

        return JdbcSink.sink(Sql,
                (preparedStatement, streamData) -> {
                    Class<?> type = streamData.getClass();
                    Field[] fieldsList = type.getDeclaredFields();
                    for(int i=0; i< fieldsList.length; i++) {
                        Field field = fieldsList[i];
                        field.setAccessible(true);
                        try{
                            System.out.println(field.getName());
                            Object value = field.get(streamData);
                            preparedStatement.setObject(i+1,value);
                        }
                        catch (Exception e){
                            System.out.println(e.getMessage());
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl( String.format("jdbc:clickhouse://%s:%s/%s",this.ckHost,this.ckPort,this.dbName))
                        .withUsername(this.user)
                        .withPassword(this.password)
                        .build()
        );
    }
}
