package com.dant.indexingengine;

import com.dant.exception.NonIndexedColumn;
import com.dant.exception.TableNotFoundException;
import com.dant.indexingengine.columns.Column;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexingEngineSingleton {
    private static final IndexingEngineSingleton INSTANCE;

    private final ArrayList<Table> tables;
    private FileManager fm;
    private final Set<String> pathsAllocated;


    static {
        INSTANCE = new IndexingEngineSingleton();
    }

    private IndexingEngineSingleton() {
        tables = new ArrayList<>();
        try {
            fm = new FileManager();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("[IndexingEngineSingleton - Constructor] Can't create file.");
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
    public void startIndexing(String fileName, String tableName) throws IOException, TableNotFoundException {

        // Files related vars
        FileInputStream fis;
        InputStreamReader isr;
        CSVReader reader;

        // Reading CSV vars
        String[] lineArray;
        String[] header = new String[0];
        Object[] castedLine;
        int i, headerLength;
        boolean isFirst = true;

        Table t;
        if ((t = getTableByName(tableName)) == null) throw new TableNotFoundException("Table" + tableName + " does not exist.");
        ArrayList<Column> indexedColumns = t.getIndexedColumns();

        // go through upload folder
        File uploadFolder = Paths.get("src", "main", "resources", "uploads").toFile();

        for (String file : Objects.requireNonNull(uploadFolder.list())) {
            if (file.contains("test")) continue;
            System.out.println("[IndexingEngineSingleton - StartIndexing] - Indexing file : " + file);

            fis = new FileInputStream(Paths.get("src", "main", "resources", "uploads", file).toFile());
            isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            reader = new CSVReader(isr);

            try {
                lineArray = reader.readNext();
                headerLength = lineArray.length;
                if (lineArray.length != getTableByName(tableName).getColumns().size()) {
                    System.out.println("Error, the file provided does not correspond to the table; fileName : " + file);
                    return;
                }
                // Check headers
                if (isFirst) {
                    header = lineArray;
                    // Setting up columns No
                    for (i = 0; i < headerLength; i++) {
                        t.getColumnByName(lineArray[i]).setColumnNo(i);
                    }
                    isFirst = false;
                } else {
                    if (!Arrays.equals(header, lineArray)) {
                        // TODO : Exception
                        System.out.println("File :" + file + " does not correspond to others files.");
                        return;
                    }
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

                    // Update indexes
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

        int indexSum = 0;
        for (Column c : t.getColumns()) {
            if (c.isIndexed()) indexSum += c.getIndex().size();
        }
        System.out.println("[IndexingEngineSingleton - StartIndexing] - Indexing ended, created :" + indexSum + "indexes");
    }


    public ArrayList<Object[]> handleQuery(Query q) throws Exception { // TODO : handle exception
        ArrayList<Column> selectedCols;
        Table t = getTableByName(q.from);

        ArrayList<Integer> resultLineNos = new ArrayList<>();
        ArrayList<Integer> otherFilter;

        ArrayList<Map.Entry<String, Map<String, Object>>> nonIndexedColsConditions = new ArrayList<>();
        int nbCond = 0;

        // Iterate through conditions
        for (Map.Entry<String, Map<String, Object>> entry : q.where.entrySet()) {
            try {
                if (entry.getValue().get("operator").equals("=")) {
                    otherFilter = t.getColumnByName(entry.getKey()).getLinesForIndex(entry.getValue().get("value"), q.limit);
                    if (resultLineNos.isEmpty()) resultLineNos = otherFilter;
                    else {
                        if (q.operator.equalsIgnoreCase("and"))
                            resultLineNos.retainAll(otherFilter); // Intersect
                        if (q.operator.equalsIgnoreCase("or")) {
                            resultLineNos = (ArrayList<Integer>)
                                    Stream.concat(resultLineNos.stream(), otherFilter.stream())
                                            .distinct()
                                            .collect(Collectors.toList());
                            Collections.sort(resultLineNos);
                        }
                    }
                    nbCond++;
                }
            } catch (NonIndexedColumn e) {
                nonIndexedColsConditions.add(entry);
            }
            // Don't stop before processing all conditions, truncate if necessary
            if (nbCond == q.where.entrySet().size() && (resultLineNos.size() >= q.limit))
                resultLineNos = (ArrayList<Integer>) resultLineNos.stream()
                        .limit(q.limit)
                        .collect(Collectors.toList());
        }

        // Handle non indexed cols
        if (nonIndexedColsConditions.size() > 0) {
            if (q.operator.equalsIgnoreCase("or")) {
                resultLineNos = handleOrQueriesNonIndexedCols(t, q, resultLineNos, nonIndexedColsConditions);
            } else {
                resultLineNos = handleANDQueriesNonIndexedCols(t, q, resultLineNos, nonIndexedColsConditions);
            }
        }


        // Used to filter response
        if (q.cols.get(0).equals("*")) selectedCols = t.getColumns();
        else selectedCols = t.getColumnsByNames(q.cols);

        ArrayList<Object[]> returnedLines = new ArrayList<>();

        if (!resultLineNos.isEmpty()) {
            returnedLines.addAll(fm.getLines(resultLineNos, t.getColumns(), selectedCols));
        }
        return new ArrayList<>(returnedLines.subList(0, Math.min(q.limit, returnedLines.size())));
    }

    public ArrayList<Integer> handleANDQueriesNonIndexedCols(Table t, Query q, ArrayList<Integer> indexedLinesNos, ArrayList<Map.Entry<String, Map<String, Object>>> nonIndexedColsConditions) {
        ArrayList<Integer> tmpList = new ArrayList<>();
        boolean matchedConditions = false;
        int runningLineNo = 0;
        int testedLines = 0;
        Object lineValue;

        while (tmpList.size() < q.limit && runningLineNo < fm.countLine()) {
            try {
                for (Object[] o : fm.getLinesInterval(testedLines, testedLines + 1000, t.getColumns(), t.getColumns())) {

                    // We check every condition to see if the line matches it, as it's an AND, we need to match every single condition
                    for (Map.Entry<String, Map<String, Object>> e : nonIndexedColsConditions) {
                        lineValue = o[t.getColumnByName(e.getKey()).getColumnNo()];

                        if (lineValue.equals(t.getColumnByName(e.getKey()).castString(e.getValue().get("value").toString()))) {
                            matchedConditions = true;
                        } else {
                            matchedConditions = false;
                            break;
                        }
                    }
                    if (matchedConditions) {
                        // If the line matched indexed cols too (in case the AND operator was on an indexed col)
                        if (indexedLinesNos.contains(runningLineNo)) tmpList.add(runningLineNo);
                    }
                    matchedConditions = false;
                    runningLineNo++;
                }

                testedLines += 1000;
            } catch (IndexOutOfBoundsException | IOException e) { // thrown by FileManager when testedLine becomes higher than total lines
                break;
            }
        }
        return tmpList;
    }

    public ArrayList<Integer> handleOrQueriesNonIndexedCols(Table t, Query q, ArrayList<Integer> indexedLinesNos, ArrayList<Map.Entry<String, Map<String, Object>>> nonIndexedColsConditions) {
        int testedLines = 0;
        int runningLineNo = 0;
        Object lineValue;
        while (indexedLinesNos.size() < q.limit) {
            try {
                for (Object[] o : fm.getLinesInterval(testedLines, testedLines + 1000, t.getColumns(), t.getColumns())) {
                    // Check for every single condition, as it's an OR operator, we only need to meet 1, so we can break
                    for (Map.Entry<String, Map<String, Object>> e : nonIndexedColsConditions) {
                        lineValue = o[t.getColumnByName(e.getKey()).getColumnNo()];

                        if (lineValue.equals(t.getColumnByName(e.getKey()).castString(e.getValue().get("value").toString()))) {
                            indexedLinesNos.add(runningLineNo);
                            break;
                        }
                    }
                    runningLineNo++;
                }

                testedLines += 1000;
            } catch (IndexOutOfBoundsException | IOException e) { // thrown by FileManager when testedLine becomes higher than total lines
                break;
            }
        }
        return indexedLinesNos;

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


    public String getNewFilePath() {
        // TODO : Find a way to clear dirs on server shutdown
        String tmpName = Paths.get("src", "main", "resources", "tmp", UUID.randomUUID().toString()).toString();
        while (pathsAllocated.contains(tmpName))
            tmpName = Paths.get("src", "main", "resources", "tmp", UUID.randomUUID().toString()).toString();
        pathsAllocated.add(tmpName);
        return tmpName;
    }
}


