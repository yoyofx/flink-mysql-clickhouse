//package org.example;
//
//import com.zaxxer.hikari.HikariConfig;
//import com.zaxxer.hikari.HikariDataSource;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//
///**
// * hikaricp 连接池示例
// * @author wanghonggang
// * 2018-10-29
// */
//public class HikariDemo {
//
//    public static void main(String[] args) {
//
//        //配置文件
//        HikariConfig hikariConfig = new HikariConfig();
////        hikariConfig.setJdbcUrl("jdbc:mysql://localhost:3306/mydata");//mysql
//        hikariConfig.setJdbcUrl("jdbc:clickhouse://192.168.15.111:8123/default");//oracle
//        hikariConfig.setDriverClassName("ru.yandex.clickhouse.ClickHouseDriver");
//        hikariConfig.setUsername("default");
//        hikariConfig.setPassword("");
//        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
//        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
//        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
//
//        HikariDataSource ds = new HikariDataSource(hikariConfig);
//        Connection conn = null;
//        Statement statement = null;
//        ResultSet rs = null;
//        try{
//
//            //创建connection
//            conn = ds.getConnection();
//            statement = conn.createStatement();
//            String sb = "INSERT INTO default.xxl_job_group (id,app_name,title,address_type,address_list,update_time,date_time,filed1,filed2)  " +
//                    "values (18,'xxl-job-executor-sample','11示例执行器',0,'null','2023-01-13 13:19:07','2023-03-03','2023-03-03 10:00:17',153.20);";
//            boolean execute = statement.execute(sb);
//            System.out.println(execute);
//            //执行sql
////            rs = statement.executeQuery("select * from xxl_job_group;");
//
//            //取数据
////            if (rs.next()){
////                System.out.println(rs.getString("title"));
////            }
//
//            statement.close();
//            //关闭connection
//            conn.close();
//            ds.close();
//        }
//        catch (SQLException e){
//            e.printStackTrace();
//        }
//
//    }
//
//}