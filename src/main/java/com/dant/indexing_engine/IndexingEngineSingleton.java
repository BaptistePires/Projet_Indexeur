package com.dant.indexing_engine;

import com.dant.entity.Column;
import com.dant.entity.Query;
import com.dant.entity.Table;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class IndexingEngineSingleton {

    private static final IndexingEngineSingleton INSTANCE;
    private Table table;
    Gson gson = new Gson();

    // TODO : Tmp -> improve after POC (Move to another class handling data) + Parse ALL colums, not just indexes
    // Will work with only one index currently
    private Map<Map<String, Object>, List<Integer>> indexedData;
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
            List<Integer> tmpValues;

            // Handling header
            line = bf.readLine();
            splitLine = line.split(",");
            for (int i = 0; i < splitLine.length; i++) {
                table.getColumnByName(splitLine[i]).setColumnNo(i);
            }
            table.mapColumnsByNo();

            int lineno = 1;
            while ((line = bf.readLine()) != null) {
                splitLine = line.split(",", -1);
                tmpIndexes = new HashMap<>();
                // Get Value of index
                for (Column c : table.getIndexedColumns()) {
                    tmpIndexes.put(c.getName(), c.castStringToType(splitLine[c.getColumnNo()]));
                }

                // Get associated occurrences or new ArrayList if none
                tmpValues = indexedData.computeIfAbsent(tmpIndexes, k -> new ArrayList<>());

                // Add this line number to occurrences
                tmpValues.add(lineno);
                lineno++;
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

    public List<JsonObject> handleQuery(Query q, String filename) throws IOException {
        // Getting line offsets from file / O(n) : Can we improve this ?
        RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "r");
        ArrayList<Integer> offsets = new ArrayList<>();
        while (randomAccessFile.readLine() != null)
            offsets.add((int) randomAccessFile.getFilePointer());

        JsonObject jsonObject;
        try {
            List<JsonObject> returnedData = new ArrayList<>();
            if (q.getType().equalsIgnoreCase("select")) {

                HashMap<String, Object> tmpIndex = new HashMap<>();
                for (Map.Entry<String, Map<String, Object>> entry : q.getConditions().entrySet()) {

                    if (entry.getValue().get("operator").equals("=")) {
                        tmpIndex.put(entry.getKey(), table.getColumnByName(entry.getKey()).castStringToType(entry.getValue().get("value").toString()));
                    }
                }
                List<Integer> lineNumbers = indexedData.get(tmpIndex);

                for (int lineNumber : lineNumbers) {
                    randomAccessFile.seek(offsets.get(lineNumber - 1));
                    String[] splitLine = randomAccessFile.readLine().split(",");
                    jsonObject = new JsonObject();
                    for (int i = 0; i < splitLine.length; i++) {
                        jsonObject.addProperty(table.getColumnByNo(i).getName(), gson.toJson(table.getColumnByNo(i).castStringToType(splitLine[i]), table.getColumnByNo(i).getType()));
                    }
                    returnedData.add(jsonObject);
                }

                return returnedData;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

}


