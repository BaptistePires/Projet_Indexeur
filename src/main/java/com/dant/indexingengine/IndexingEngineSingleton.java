package com.dant.indexingengine;

import com.dant.entity.Column;
import com.dant.entity.Table;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO : Tmp -> improve after POC (Move to another class handling data) + Parse ALL colums, not just indexes
// Will work with only one index currently
public class IndexingEngineSingleton {

    private static final IndexingEngineSingleton INSTANCE;

    private Table table;

	private Map<Map<String, Object>, List<Integer>> indexedData;
	private ArrayList<Integer> offsets = new ArrayList<>();

	private RandomAccessFile randomAccessFile;
	private Gson gson = new Gson();
	private String filePath;

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
		this.filePath = filePath;
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
		return !table.getIndexedColumns().isEmpty() && !indexed && !indexing && !error;
	}

	public boolean canQuery() {
		return indexed && !indexing && !error;
	}

	public ArrayList<Integer> getOffsets() {
		return offsets;
	}

	public RandomAccessFile getRandomAccessFile() {
		return randomAccessFile;
	}

	public Gson getGson() {
		return gson;
	}

	public Map<Map<String, Object>, List<Integer>> getIndexedData() {
		return indexedData;
	}
}


