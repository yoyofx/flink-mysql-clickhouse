package org.example.fx;


import com.clickhouse.jdbc.ClickHouseDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.*;
import java.util.*;

/**
 * 核心类
 * 写入ck
 */
public class CancalFormatCkSink extends RichSinkFunction<List<CancalBinlogRow>> {

    private static Log logger = LogFactory.getLog(CancalFormatCkSink.class.getName());

    private Connection conn = null;

    //    Statement statement = null;
    private String ckHost = "";
    private String ckPort = "";
    private String user = "";
    private String password = "";
    private String dbName = "";

    //匹配此类型，不在拼接引号
    private static List fliedType = new ArrayList();

    //数值类型
    static {
        fliedType.add("tinyint");
        fliedType.add("smallint");
        fliedType.add("mediumint");
        fliedType.add("int");
        fliedType.add("integer");
        fliedType.add("bigint");
        fliedType.add("float");
        fliedType.add("double");
        fliedType.add("decimal");
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
        Properties properties = new Properties();
//        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        conn = dataSource.getConnection(this.user, this.password);
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
        Statement stmt = conn.createStatement();

        try {
            StringBuffer sql = new StringBuffer();
            for (CancalBinlogRow row : rows) {
                String type = row.getType();
                String sourceDatabase = row.getDatabase();
                logger.error("sql数据类型{}" + type+"sourceDatabase:"+sourceDatabase);
                StringBuffer sb = new StringBuffer();
                sb.append("INSERT INTO ");

                //匹配源数据库
                if (!"chehou_data_report".equals(sourceDatabase)) {
                    continue;
                }
                //暂时先直接定义目标库名称
                String database = "yccb_analysis";
                //类型不存在，剔除sql
                if (type == null) {
                    //类型为空跳出
                    continue;
                }
                // 识别为 insert 或者 update 继续后续逻辑
                if (!("insert".equals(type.toLowerCase()) || "update".equals(type.toLowerCase()))) {
                    continue;
                }
//                logger.info("sql类型：" + type);
                String table = row.getTable();
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
                            dataBuffer.append(data.get(key)).append(",");
                        } else {
                            dataBuffer.append("\'" + data.get(key) + "\'").append(",");

                        }

//                        System.out.println("key:" + key + "," + " value:" + data.get(key));
                    }
                }
                //追加字段
                sb.append(filedsBuffer.substring(0, filedsBuffer.length() - 1)).append(") ");
                sb.append(" values ");
                sb.append("(").append(dataBuffer.substring(0, dataBuffer.length() - 1)).append(")");

                stmt.addBatch(sb.toString());
                logger.info("sql:" + String.valueOf(sb));
                sql.append(sb).append(";").append("\n");
            }

            logger.info("sql语句:" + sql.toString());
            int[] ints = stmt.executeBatch();
            logger.info("插入成功条目数:" + Arrays.toString(ints));
            conn.commit();
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }


}

