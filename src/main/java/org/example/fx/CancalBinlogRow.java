
package org.example.fx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import javax.annotation.Generated;

/**
 * Auto-generated: 2023-03-01 15:35:19
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CancalBinlogRow {

    private List<LinkedHashMap<String,Object>> data;
    private String database;
    private int id;
    private boolean isDdl;
    private String table;
    private long ts;
    private String type;
    private LinkedHashMap<String, String> mysqlType;

    public LinkedHashMap<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(LinkedHashMap<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public void setData(List<LinkedHashMap<String,Object>> data) {
        this.data = data;
    }
    public List<LinkedHashMap<String,Object>> getData() {
        return data;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
    public String getDatabase() {
        return database;
    }

    public void setId(int id) {
        this.id = id;
    }
    public int getId() {
        return id;
    }

    public void setIsDdl(boolean isDdl) {
        this.isDdl = isDdl;
    }
    public boolean getIsDdl() {
        return isDdl;
    }

    public void setTable(String table) {
        this.table = table;
    }
    public String getTable() {
        return table;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
    public long getTs() {
        return ts;
    }

    public void setType(String type) {
        this.type = type;
    }
    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "CancalBinlogRow{" +
                "data=" + data +
                ", database='" + database + '\'' +
                ", id=" + id +
                ", isDdl=" + isDdl +
                ", table='" + table + '\'' +
                ", ts=" + ts +
                ", type='" + type + '\'' +
                ", mysqlType='" + mysqlType + '\'' +
                '}';
    }
}