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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CancalFormatCkSink extends RichSinkFunction<List<CancalBinlogRow>> {
    private ClickHouseConnection conn = null;
    private String ckHost = "";
    private String ckPort = "";
    private String user = "";
    private String password = "";
    private String dbName = "";

    public CancalFormatCkSink withHost(String ckHost,String ckPort) {
        this.ckHost = ckHost;
        this.ckPort = ckPort;
        return this;
    }

    public CancalFormatCkSink withAuth(String db,String userName,String passWord) {
        this.dbName = db;
        this.user = userName;
        this.password = passWord;
        return this;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String url = String.format("jdbc:clickhouse://%s:%s/%s",this.ckHost,this.ckPort,this.dbName);
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(this.user);
        properties.setPassword(this.password);
        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        conn = dataSource.getConnection();
        conn.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null)
        {
            conn.close();
        }
    }

    @Override
    public void invoke(List<CancalBinlogRow> rows, Context context) throws Exception {
        PreparedStatement ps = null;
        try {
            for(CancalBinlogRow row : rows) {
                ps = conn.prepareStatement("");
                // add params
//                ps.setObject();
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            ps.close();
        }
        catch (SQLException e){
            System.out.println(e.getMessage());
        }
    }
}

