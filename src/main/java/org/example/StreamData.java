package org.example;


import org.example.fx.Table;

@Table("test_local")
public class StreamData {
    private Integer id;
    private String key1;
    private boolean value1;
    private Long key2;
    private Double value2;

    public StreamData(Integer id, String key1,boolean value1, Long key2, Double value2) {
        this.id = id;
        this.key1 = key1;
        this.value1 = value1;
        this.key2 = key2;
        this.value2 = value2;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getKey1() {
        return key1;
    }

    public void setKey1(String key1) {
        this.key1 = key1;
    }

    public boolean isValue1() {
        return value1;
    }

    public void setValue1(boolean value1) {
        this.value1 = value1;
    }

    public Long getKey2() {
        return key2;
    }

    public void setKey2(Long key2) {
        this.key2 = key2;
    }

    public Double getValue2() {
        return value2;
    }

    public void setValue2(Double value2) {
        this.value2 = value2;
    }
}
