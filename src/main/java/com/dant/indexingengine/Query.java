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
    Map<String, Map<String, Object>> conditions;

    public Query(String type, List<String> cols, Map<String, Map<String, Object>> conditions) {
        this.type = type;
        this.cols = cols;
        this.conditions = conditions;
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

    public Map<String, Map<String, Object>> getConditions() {
        return conditions;
    }

    public void setConditions(Map<String, Map<String, Object>> conditions) {
        this.conditions = conditions;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query \n ------- ");
        sb.append("Type    : ").append(type).append("\n");
        sb.append("Columns : ").append(cols).append("\n");
        sb.append("Conditions : ").append("\n");
        for (Map.Entry<String, Map<String, Object>> e : conditions.entrySet()) {
            sb.append("\t").append(e.getKey()).append("\n");
            for (Map.Entry<String, Object> subEntry : e.getValue().entrySet()) {
                sb.append("\t\t").append(subEntry.getKey()).append(" : ").append(subEntry.getValue()).append("\n");
            }
        }
        return sb.toString();
    }
}
