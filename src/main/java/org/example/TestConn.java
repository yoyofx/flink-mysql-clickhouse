//package org.example;
//
//import org.example.fx.CancalFormatCkSink;
//import ru.yandex.clickhouse.ClickHouseConnection;
//import ru.yandex.clickhouse.ClickHouseDataSource;
//import ru.yandex.clickhouse.settings.ClickHouseProperties;
//
//import javax.xml.crypto.Data;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//
//public class TestConn {
//
//    public static void main(String[] args) throws SQLException, ParseException {
//        ClickHouseConnection conn = null;
//
//
//        String url = String.format("jdbc:clickhouse://192.168.15.111:8123/default?keepAliveTimeout=300000&socket_timeout=300000&dataTransferTimeout=300000");
//        ClickHouseProperties properties = new ClickHouseProperties();
//        properties.setUser("default");
////        properties.setPassword(this.password);
////        properties.setSessionId("default-session-id");
//
//        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
//        conn = dataSource.getConnection();
//        conn.setAutoCommit(false);
//
//        String sb = "INSERT INTO default.xxl_job_group (id,app_name,title,address_type,address_list,update_time,date_time,filed1,filed2) " +
//                "values " +
//                "(1611,?,'11示例执行器',0,'null',?,null,null,153.20);";
//
//        PreparedStatement ps = conn.prepareStatement(sb.toString());
//
//
//        SimpleDateFormat sfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date parse = sfm.parse("2023-01-13 13:19:07");
//
//        ps.setString(1, "2023-03-03 10:00:17");
//        ps.setTimestamp(2, new java.sql.Timestamp(parse.getTime()));
//
//        ps.addBatch();
//        System.out.println("sqlsqlsqlsqlsqlsqlsqlsql:" + sb);
//        ps.executeBatch();
//        conn.commit();
//        ps.close();
//    }
//}
