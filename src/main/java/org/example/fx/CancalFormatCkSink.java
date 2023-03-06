package org.example.fx;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class CancalFormatCkSink extends RichSinkFunction<List<CancalBinlogRow>> {
    private ClickHouseConnection conn = null;
    private String ckHost = "";
    private String ckPort = "";
    private String user = "";
    private String password = "";
    private String dbName = "";

    private static List fliedType = new ArrayList();
    static {
        fliedType.add("varchar");
        fliedType.add("char");
        fliedType.add("text");
        fliedType.add("datetime");
        fliedType.add("date");
        fliedType.add("timestamp");
    }

    public CancalFormatCkSink withHost(String ckHost, String ckPort) {
        this.ckHost = ckHost;
        this.ckPort = ckPort;
        return this;
    }

    public CancalFormatCkSink withAuth(String db, String userName, String passWord) {
        this.dbName = db;
        this.user = userName;
        this.password = passWord;
        return this;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String url = String.format("jdbc:clickhouse://%s:%s/%s?keepAliveTimeout=300000&socket_timeout=300000&dataTransferTimeout=300000", this.ckHost, this.ckPort, this.dbName);
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(this.user);
        properties.setPassword(this.password);
//        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        conn = dataSource.getConnection();
        conn.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public void invoke(List<CancalBinlogRow> rows, Context context) throws Exception {
        PreparedStatement ps = null;
        try {
            StringBuffer sb = null;
            for (CancalBinlogRow row : rows) {
                sb = new StringBuffer("INSERT INTO ");
//                String database = row.getDatabase();
                String database = "default";
                String table = row.getTable();
                String type = row.getType();
                if (type == null || !"insert".equals(type.toLowerCase())) {
                    return;
                }
                sb.append(database).append(".").append(table).append(" (");
                //获取每行的数据
                List<LinkedHashMap<String, Object>> datas = row.getData();

                //字段类型
                LinkedHashMap<String, String> mysqlType = row.getMysqlType();
                //解析字段
                StringBuffer filedsBuffer = new StringBuffer();
                StringBuffer dataBuffer = new StringBuffer();
                for (LinkedHashMap<String, Object> data : datas) {
                    Iterator<String> keys = data.keySet().iterator();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        filedsBuffer.append(key).append(",");
                        if (fliedType.contains(mysqlType.get(key).split("\\(")[0])){
                            dataBuffer.append("\'"+data.get(key)+"\'").append(",");
                        }else {
                            dataBuffer.append(data.get(key)).append(",");
                        }

                        System.out.println("key:" + key + "," + " value:" + data.get(key));
                    }
                }
                //追加字段
                sb.append(filedsBuffer.substring(0, filedsBuffer.length() - 1)).append(") ");
                sb.append(" values ");
                sb.append("(").append(dataBuffer.substring(0, dataBuffer.length() - 1)).append(");");
                // add params
//                ps.setObject();

            }
            ps = conn.prepareStatement(sb.toString());

            ps.addBatch();
            System.out.println("sqlsqlsqlsqlsqlsqlsqlsql:"+sb);
            ps.executeBatch();
            conn.commit();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }



}

