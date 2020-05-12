package com.dant.indexing_engine;

import com.dant.entity.Column;
import com.dant.entity.Query;
import com.dant.entity.Table;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

// TODO : Tmp -> improve after POC (Move to another class handling data) + Parse ALL colums, not just indexes
// Will work with only one index currently
public class IndexingEngineSingleton {

    private static final IndexingEngineSingleton INSTANCE;

    private Table table;

	private Map<Map<String, Object>, List<Integer>> indexedData;
	private ArrayList<Integer> offsets = new ArrayList<>();

	private RandomAccessFile randomAccessFile;
	private Gson gson = new Gson();

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

	/**
	 * Indexes the file and keeps line offsets for future queries
	 * @param filePath path to .csv file;
	 * @throws {@link IOException}
	 */
	public void startIndexing(String filePath) throws IOException {
	    randomAccessFile = new RandomAccessFile(filePath, "r");
        try {
            String line;
            String[] splitLine;

            Map<String, Object> tmpIndexes;
            List<Integer> tmpValues;

            // Handling header
            line = randomAccessFile.readLine();
	        offsets.add((int) randomAccessFile.getFilePointer());
            splitLine = line.split(",");
            for (int i = 0; i < splitLine.length; i++) {
                table.getColumnByName(splitLine[i]).setColumnNo(i);
            }
            table.mapColumnsByNo();

            int lineno = 1;
            while ((line = randomAccessFile.readLine()) != null) {
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

                // Save to offset of this line for queries
	            offsets.add((int) randomAccessFile.getFilePointer());
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

	/**
	 * For handling a query on the .csv
	 * @param q : Query object
	 * @return {@link List<JsonObject>} : Query results
	 */
	public List<JsonObject> handleQuery(Query q) {
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

	public Table getTable() {
		return table;
	}

	public boolean isIndexed() {
		return indexed;
	}

	public boolean isAvailable() {
		return !indexing;
	}

	public boolean canIndex() {
		return table.getIndexedColumns().size() > 0 && !indexed && !indexing && !error;
	}

	public boolean canQuery() {
		return indexed && !indexing && !error;
	}

}


