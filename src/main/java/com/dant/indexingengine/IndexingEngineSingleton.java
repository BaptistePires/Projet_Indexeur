package com.dant.indexingengine;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.Data;
import org.apache.commons.lang3.SerializationUtils;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.*;

// TODO : Tmp -> improve after POC (Move to another class handling data) + Parse ALL colums, not just indexes
// Will work with only one index currently
public class IndexingEngineSingleton {

    private static final IndexingEngineSingleton INSTANCE;

    private ArrayList<Table> tables;
    private FileManager fm;
    private HashMap<Integer, Integer> index;

    private boolean indexed;
    private boolean indexing;
    private boolean error;

    static {
        INSTANCE = new IndexingEngineSingleton();
    }

    private IndexingEngineSingleton() {
        tables = new ArrayList<>();
        indexed = false;
        indexing = false;
        try {
            fm = new FileManager();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }

    public static IndexingEngineSingleton getInstance() {
        return INSTANCE;
    }

    /**
     * Indexes the file and keeps line offsets for future queries
     *
     * @param filePath path to .csv file;
     * @throws {@link IOException}
     */
    public void startIndexing(String filePath, String tableName) throws IOException {
        // Files related vars
//        String fileName = "src/main/resources/csv/yellow_tripdata_2019-01.csv";
        String fileName = "src/main/resources/csv/test.csv";
        FileInputStream fis = new FileInputStream(fileName);
        InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
        CSVReader reader = new CSVReader(isr);

        // Reading CSV vars
        String[] lineArray;
        String line;
        Object[] castedLine;
        int i, headerLength;

        Table t;
        if ((t = getTableByName(tableName)) == null) return;


        try {
            lineArray = reader.readNext();
            headerLength = lineArray.length;
            if (lineArray.length != getTableByName(tableName).getColumns().size()) {
                System.out.println("Error, the file provided does not correspond to the table;");
                return;
            }


            // Setting up columns No
            for (i = 0; i < headerLength; i++) {
                t.getColumnByName(lineArray[i]).setColumnNo(i);
            }
            
            t.sortColumnsByNo();
            
            while ((lineArray = reader.readNext()) != null) {
                // Cast data
                castedLine = new Object[headerLength];
                for (i = 0; i < headerLength; i++) {
                    castedLine[i] = t.getColumns().get(i).castAndUpdateMetaData(lineArray[i]);
                }
                // Write line on disk
                long noLine = fm.writeLine(castedLine);

                // Update indexes
                for(Map.Entry<Column[], SimpleIndex> entry: t.getIndexes().entrySet()){
                    ArrayList<String> lst = new ArrayList<>();
                    for(Column c : entry.getKey()){
                        lst.add(lineArray[c.getColumnNo()]);
                    }
                    String idx = String.join(",", lst);
                    entry.getValue().index(idx,(int) noLine);
                }

            }

        } catch (CsvValidationException e) {
            e.printStackTrace();
            System.out.println("bizarre bizarre");
        } catch (Exception e) {
            // ->>> t.mapColumnByNo; handle exception better
            System.out.println("herre");
            e.printStackTrace();
        }

    }


    public Object[] handleQuery(Query q) {
        Object[] lines = new Object[1];
        Table t = getTableByName(q.table);

        // Build tmp indexes
        String[] indexKeyArray = new String[q.conditions.size()];

        // Iterate through conditions
        for(Map.Entry<String, Map<String, Object>> entry: q.conditions.entrySet()) {

        }
        return lines;
    }

    // deubg
    public ArrayList<Object[]> getallLines() {
        return fm.getAllLines();
    }

    public ArrayList<Table> getTables() {
        return tables;
    }

    public Table getTableByName(String name) {
        for (Table t : tables) {
            if (t.getName().equals(name)) {
                return t;
            }
        }
        return null;
    }

    public void addTable(Table t) {
        this.tables.add(t);
    }

    public boolean isIndexed() {
        return indexed;
    }

    public boolean isAvailable() {
        return !indexing;
    }

    public boolean canIndex() {
        return true;
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


