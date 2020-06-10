package com.dant.indexingengine;

import com.dant.exception.NonIndexedColumn;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;

// TODO : Tmp -> improve after POC (Move to another class handling data) + Parse ALL colums, not just indexes
// Will work with only one index currently
public class IndexingEngineSingleton {
    private static final IndexingEngineSingleton INSTANCE;

    private final ArrayList<Table> tables;
    private FileManager fm;
    private HashMap<Integer, Integer> index;
    private final Set<String> pathsAllocated;

    private final boolean indexed;
    private final boolean indexing;
    private boolean error;

    private int lineNum;

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
        pathsAllocated = new HashSet<>();

    }

    public static IndexingEngineSingleton getInstance() {
        return INSTANCE;
    }

    /**
     * Indexes the file and keeps line offsets for future queries
     *
     * @param fileName path to .csv file;
     * @throws {@link IOException}
     */
    public void startIndexing(String fileName, String tableName) throws IOException {
        // Files related vars
//        String fileName = "src/main/resources/csv/test.csv";
        //String fileName = "src/main/resources/csv/yellow_tripdata_2019-01.csv";
        FileInputStream fis = new FileInputStream(fileName);
        InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
        CSVReader reader = new CSVReader(isr);

        String outputFilePath = "src/main/resources/csv/DataOutputFile.bin";
        DataOutputStream out = new DataOutputStream((new BufferedOutputStream(new FileOutputStream(outputFilePath))));

        // Reading CSV vars
        String[] lineArray;
        String line;
        Object[] castedLine;
        int i, headerLength;

        Table t;
        //TODO : Exception table does not exist
        if ((t = getTableByName(tableName)) == null) return;
        ArrayList<Column> indexedColumns = t.getIndexedColumns();


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
            long noLine = 0;
            while ((lineArray = reader.readNext()) != null) {
                // Cast data
                castedLine = new Object[headerLength];
                for (i = 0; i < headerLength; i++) {
                    castedLine[i] = t.getColumns().get(i).castAndUpdateMetaData(lineArray[i]);
                }
                // Write line on disk
                noLine = fm.writeLine(castedLine, t.getColumns());

                //                 Update indexes
                for (Column c : indexedColumns) {
//                    castedLine[c.getColumnNo()] = c.castAndUpdateMetaData(lineArray[c.getColumnNo()]);
                    c.index(castedLine[c.getColumnNo()], (int) noLine);
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


    public ArrayList<Object[]> handleQuery(Query q) throws Exception {
        ArrayList<Object[]> lines;
        ArrayList<Column> selectedCols;
        Table t = getTableByName(q.from);

        ArrayList<Integer> linesNumber = new ArrayList<>();
        ArrayList<Object[]> returnedLines = new ArrayList<>();
        ArrayList<Map.Entry<String, Map<String, Object>>> nonIndexedColsConditions = new ArrayList<>();
        // Iterate through conditions
        for (Map.Entry<String, Map<String, Object>> entry : q.conditions.entrySet()) {
            try {
                if (entry.getValue().get("operator").equals("=")) {
                    linesNumber = t.getColumnByName(entry.getKey()).getLinesForIndex(entry.getValue().get("value"), q.limit);
                }
            } catch (NonIndexedColumn e) {
                nonIndexedColsConditions.add(entry);
            }
            if (linesNumber.size() >= q.limit) break;
        }

        // Handle non indexed cols
        int testedLines = 0;
        Object lineValue;
        while (linesNumber.size() + returnedLines.size() < q.limit) {
            try {
                for (Object[] o : fm.getLinesInterval(testedLines, testedLines + 1000, t.getColumns(), t.getColumns())) {
                    for (Map.Entry<String, Map<String, Object>> e : nonIndexedColsConditions) {
                        lineValue = o[t.getColumnByName(e.getKey()).getColumnNo()];

                        if (lineValue.equals(t.getColumnByName(e.getKey()).castString(e.getValue().get("value").toString()))) {
                            returnedLines.add(o);
                        }
                    }
                }

                testedLines += 1000;
            } catch (IndexOutOfBoundsException e) { // thrown by FileManager when testedLine becomes higher than total lines
                break;
            }
        }


        if (q.cols.get(0).equals("*")) selectedCols = t.getColumns();
        else selectedCols = t.getColumnsByNames(q.cols);

        if (!linesNumber.isEmpty()) {
            returnedLines.addAll(fm.getLines(linesNumber, t.getColumns(), selectedCols));
        }
        return new ArrayList<>(returnedLines.subList(0, Math.min(q.limit, returnedLines.size())));
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

    public String getNewFilePath() {
        // TODO : Find a way to clear dirs on server shutdown
        String tmpName = Paths.get("src", "main", "resources", "tmp", UUID.randomUUID().toString()).toString();
        while (pathsAllocated.contains(tmpName))
            tmpName = Paths.get("src", "main", "resources", "tmp", UUID.randomUUID().toString()).toString();
        pathsAllocated.add(tmpName);
        return tmpName;
    }


}


