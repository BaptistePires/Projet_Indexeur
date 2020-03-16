package com.dant.indexing_engine;

import com.dant.entity.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexingEngineSingleton {

    private static final IndexingEngineSingleton INSTANCE;
    private Table table;

    // TODO : Tmp -> improve after POC (Move to another class handling data)
    // Will work with only one index currently
    private Map<List<?>, List<Object[]>> indexedData;
    private boolean indexed;
    private boolean indexing;
    private boolean error;

    static {
        INSTANCE = new IndexingEngineSingleton();
    }

    private IndexingEngineSingleton() {
        table = new Table();
        indexed = false;
        indexing = false;
        indexedData = new HashMap<>();
    }

    public static IndexingEngineSingleton getInstance() {
        return INSTANCE;
    }

    public Table getTable() {
        return table;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public boolean isAvailable() {
        return !indexing;
    }

    public void startIndexing() {
    }

    public boolean canIndex() {
        return table.getIndexedColumns().size() > 0 && !indexed && !indexing && !error;
    }

    public boolean canQuery() {
        return indexed && !indexing && !error;
    }
}
