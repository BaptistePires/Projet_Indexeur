package com.dant.indexingengine;

import com.google.gson.annotations.Expose;

import java.util.List;
import java.util.Map;

public class Query {

    @Expose
    String type;

    @Expose
    List<String> cols;

    @Expose
    Map<String, Map<String, Object>> where;

    @Expose
    int limit;

    @Expose
    String from;

    @Expose
    String operator;


    public Query(String type, List<String> cols, Map<String, Map<String, Object>> where, int limit, String from, String operator) {
        this.type = type;
        this.cols = cols;
        this.where = where;
        this.limit = limit;
        this.from = from;
        this.operator = operator;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getCols() {
        return cols;
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }

    public Map<String, Map<String, Object>> getWhere() {
        return where;
    }

    public void setWhere(Map<String, Map<String, Object>> where) {
        this.where = where;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query \n ------- ");
        sb.append("Type    : ").append(type).append("\n");
        sb.append("Columns : ").append(cols).append("\n");
        sb.append("Limit    : ").append(limit).append("\n");
        sb.append("Table    : ").append(from).append("\n");
        sb.append("Conditions : ").append("\n");
        for (Map.Entry<String, Map<String, Object>> e : where.entrySet()) {
            sb.append("\t").append(e.getKey()).append("\n");
            for (Map.Entry<String, Object> subEntry : e.getValue().entrySet()) {
                sb.append("\t\t").append(subEntry.getKey()).append(" : ").append(subEntry.getValue()).append("\n");
            }
        }
        return sb.toString();
    }
}
