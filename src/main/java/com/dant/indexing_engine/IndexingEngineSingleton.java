package com.dant.indexing_engine;

import com.dant.entity.Column;
import com.dant.entity.Query;
import com.dant.entity.Table;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexingEngineSingleton {

    private static final IndexingEngineSingleton INSTANCE;
    private Table table;

    // TODO : Tmp -> improve after POC (Move to another class handling data) + Parse ALL colums, not just indexes
    // Will work with only one index currently
    private Map<Map<String, Object>, List<JsonObject>> indexedData;
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
        String filePath = Paths.get(".", "src", "main", "resources", "csv", "test.csv").toString();
        File f = new File(filePath);
        try {
            BufferedReader bf = new BufferedReader(new FileReader(f));

            String line;
            String[] splitLine;

            Map<String, Object> tmpIndexes;
            List<JsonObject> tmpValues;

            // Handling header
            line = bf.readLine();
            splitLine = line.split(",");
            for (int i = 0; i < splitLine.length; i++) {
                table.getColumnByName(splitLine[i]).setColumnNo(i);
            }
            table.mapColumnsByNo();
            JsonObject jsonObject;
            Gson gson = new Gson();

            while ((line = bf.readLine()) != null) {
                splitLine = line.split(",", -1);
                tmpIndexes = new HashMap<>();
                for (Column c : table.getIndexedColumns()) {
                    tmpIndexes.put(c.getName(), c.castStringToType(splitLine[c.getColumnNo()]));
                }

                tmpValues = indexedData.computeIfAbsent(tmpIndexes, k -> new ArrayList<>());
                jsonObject = new JsonObject();
                for (int i = 0; i < splitLine.length; i++) {
                    jsonObject.addProperty(table.getColumnByNo(i).getName(), gson.toJson(table.getColumnByNo(i).castStringToType(splitLine[i]), table.getColumnByNo(i).getType()));

                }
                tmpValues.add(jsonObject);

            }
            indexed = true;
            indexing = false;
            error = false;

        } catch (FileNotFoundException e) {
            // TODO : tmp -> handle errors
            e.printStackTrace();
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }


    }

    public boolean canIndex() {
        return table.getIndexedColumns().size() > 0 && !indexed && !indexing && !error;
    }

    public boolean canQuery() {
        return indexed && !indexing && !error;
    }

    public List<JsonObject> handleQuery(Query q) {
        try {
            List<JsonObject> returnedData = new ArrayList<>();
            if (q.getType().toLowerCase().equals("select")) {

                HashMap<String, Object> tmpIndex = new HashMap<>();
                for (Map.Entry<String, Map<String, Object>> entry : q.getConditions().entrySet()) {

                    if (entry.getValue().get("operator").equals("=")) {
                        tmpIndex.put(entry.getKey(), table.getColumnByName(entry.getKey()).castStringToType(entry.getValue().get("value").toString()));
                    }
                }
                returnedData.addAll(indexedData.get(tmpIndex));
                return returnedData;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();

    }

}
