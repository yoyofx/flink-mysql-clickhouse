package org.example.fx;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;


import java.sql.*;
import java.util.*;

public class CancalFormatCkSink extends RichSinkFunction<List<CancalBinlogRow>> {

    HikariDataSource ds = null;
    private Connection conn = null;

    //    Statement statement = null;
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

        //配置文件
        HikariConfig hikariConfig = new HikariConfig();
//        hikariConfig.setJdbcUrl("jdbc:mysql://localhost:3306/mydata");//mysql
        hikariConfig.setJdbcUrl(String.format("jdbc:clickhouse://%s:%s/%s", this.ckHost, this.ckPort, this.dbName));//oracle
        hikariConfig.setDriverClassName("ru.yandex.clickhouse.ClickHouseDriver");
        hikariConfig.setUsername(this.user);
        hikariConfig.setPassword("");
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        ds = new HikariDataSource(hikariConfig);
        conn = ds.getConnection();
        conn.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ds != null) {
            ds.close();
        }
    }

    @Override
    public void invoke(List<CancalBinlogRow> rows, Context context) throws Exception {
        Statement ps = conn.createStatement();

        try {
            StringBuffer sb = new StringBuffer();
            for (CancalBinlogRow row : rows) {

                sb.append("INSERT INTO ");
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
                        if (fliedType.contains(mysqlType.get(key).split("\\(")[0])) {
                            dataBuffer.append("\'" + data.get(key) + "\'").append(",");
                        } else {
                            dataBuffer.append(data.get(key)).append(",");
                        }

                        System.out.println("key:" + key + "," + " value:" + data.get(key));
                    }
                }
                //追加字段
                sb.append(filedsBuffer.substring(0, filedsBuffer.length() - 1)).append(") ");
                sb.append(" values ");
                sb.append("(").append(dataBuffer.substring(0, dataBuffer.length() - 1)).append(")");
                // add params
//                ps.setObject();
//            ps.execute(sb.toString());
                ps.addBatch(sb.toString());
            }

            System.out.println("sqlsqlsqlsqlsqlsqlsqlsql:" + sb);
            ps.executeBatch();
            conn.commit();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }


}

