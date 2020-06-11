package com.dant.indexingengine;

import com.dant.exception.NoDataException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class QueryHandler {

    private static final QueryHandler INSTANCE;

    static {
        INSTANCE = new QueryHandler();
    }

    private final IndexingEngineSingleton indexer = IndexingEngineSingleton.getInstance();

    private QueryHandler() {
    }

    public static QueryHandler getInstance() {
        return INSTANCE;
    }

    /**
     * For handling a query on the .csv
     *
     * @param q : Query object
     * @return {@link List <JsonObject>} : Query results
     */
    public JsonObject handleQuery(Query q) throws NoDataException {
        if (!q.getType().equalsIgnoreCase("select")) throw new NoDataException();
        JsonObject returnedData = new JsonObject();
        if (q.cols.get(0).equals("*"))
            returnedData.add("columns", new Gson().toJsonTree(indexer.getTableByName(q.from).getColumnsName()));
        else
            returnedData.add("columns", new Gson().toJsonTree(q.cols));
        ArrayList<Object[]> lines = indexer.handleQuery(q);

        returnedData.add("count", new Gson().toJsonTree(lines.size()));
        returnedData.add("lines", new Gson().toJsonTree(lines));
        returnedData.add("initial_query", new Gson().toJsonTree(q));
        return returnedData;
    }

}
