package com.dant.indexingengine;

import java.util.ArrayList;
import java.util.HashMap;

public class SimpleIndex {

    private String colName;
    private HashMap<Object, ArrayList<Integer>> indexedData;

    public SimpleIndex() {
        indexedData = new HashMap<>();
    }

    public void index(Object o, int noLine) {
        indexedData.computeIfAbsent(o, k -> new ArrayList<>());
        indexedData.get(o).add(noLine);
    }

    public ArrayList<Integer> get(Object o) {
        return indexedData.containsKey(o) ? indexedData.get(o) : new ArrayList<>();
    }
}
