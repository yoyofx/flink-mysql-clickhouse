package org.example.fx;


import com.clickhouse.jdbc.ClickHouseDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;



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


//        String url = "jdbc:ch://192.168.15.111:8123/default";
        String url = String.format("jdbc:ch://%s:%s/%s", this.ckHost, this.ckPort, this.dbName);
        Properties  properties = new Properties ();
//        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        conn = dataSource.getConnection(this.user,this.password);
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
        Statement stmt = conn.createStatement();

        try {

            for (CancalBinlogRow row : rows) {
                StringBuffer sb = new StringBuffer();
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

                stmt.addBatch(sb.toString());
                System.out.println("sql:" + sb);
            }


            int[] ints = stmt.executeBatch();
            System.out.println("ints:"+ Arrays.toString(ints));
            conn.commit();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }


}

