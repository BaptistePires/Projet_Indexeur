package com.dant.indexingengine;

import com.dant.indexingengine.columns.Column;
import org.apache.commons.lang3.SerializationUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileManager {


    private long countLines;
    private final RandomAccessFile dataSavedToDisk;
    private final RandomAccessFile linesPositionsInFile;
    private final int BYTE_SIZE_LONG_ARRAY = 43;

    public FileManager() throws FileNotFoundException {
        dataSavedToDisk = new RandomAccessFile(Paths.get("src", "main", "resources", "tmp", "savedLines").toString(), "rw");
        linesPositionsInFile = new RandomAccessFile(Paths.get("src", "main", "resources", "tmp", "linesPositions").toString(), "rw");

    }

    public long writeLine(Object[] line, ArrayList<Column> columns) throws IOException {

        byte[] data = SerializationUtils.serialize(line);
        long startPos = dataSavedToDisk.length();
        if (startPos > 0) ++startPos;
        long lineSize = 0;
        dataSavedToDisk.seek(startPos);
        for (int i = 0; i < line.length; i++) {
            lineSize += columns.get(i).writeToFile(dataSavedToDisk, line[i]);
        }
        savePositionsToFile(new long[]{startPos, lineSize});
        dataSavedToDisk.seek(startPos);
        countLines++;
        return countLines - 1;
    }


    public Object[] readline(int no, ArrayList<Column> allCols, ArrayList<Column> selectedCols) throws IOException {
        long[] linePos = readPositionOfLine(no);
        dataSavedToDisk.seek(linePos[0]);
        Object[] line = new Object[allCols.size()];
        Object[] returnedLine = new Object[selectedCols.size()];
        for (int i = 0; i < allCols.size(); i++) {
            line[i] = allCols.get(i).readFromFile(dataSavedToDisk);
        }
        for (int i = 0; i < selectedCols.size(); i++) {
            returnedLine[i] = line[selectedCols.get(i).getColumnNo()];
        }
        return returnedLine;
    }

    private void savePositionsToFile(long[] positions) throws IOException {
        long startPosition = BYTE_SIZE_LONG_ARRAY * countLines;
        if (countLines > 1) startPosition++;
        linesPositionsInFile.seek(startPosition);
        byte[] positionArrayBytes = SerializationUtils.serialize(positions);
        linesPositionsInFile.write(positionArrayBytes);
    }

    private long[] readPositionOfLine(int noLine) throws IOException {

        long startPosition = BYTE_SIZE_LONG_ARRAY * noLine;
        if (noLine > 1) startPosition++;
        byte[] positionArrayBytes = new byte[BYTE_SIZE_LONG_ARRAY];
        linesPositionsInFile.seek(startPosition);
        linesPositionsInFile.read(positionArrayBytes, 0, BYTE_SIZE_LONG_ARRAY);
        return SerializationUtils.deserialize(positionArrayBytes);
    }

    public int countLine() {
        return (int) countLines;
    }

    public long size() throws IOException {
        return dataSavedToDisk.length();
    }


    public ArrayList<Object[]> getLines(List<Integer> lineNos, ArrayList<Column> allCols, ArrayList<Column> selectedCols) {
        ArrayList<Object[]> linesList = new ArrayList<>();
        try {
            for (int i = 0; i < lineNos.size(); i++) {
                linesList.add(readline(lineNos.get(i), allCols, selectedCols));
            }
            return linesList;
        } catch (Exception e) {
            e.printStackTrace();
            return linesList;
        }
    }

    public ArrayList<Object[]> getLinesInterval(int start, int end, ArrayList<Column> allCols, ArrayList<Column> selectedCols) throws Exception {
        if (end < start || start < 0 || start > countLines)
            throw new IndexOutOfBoundsException("[FileManager - getLinesInterval] Issue with start and stop, start :" + start + " end :" + end);

        ArrayList<Object[]> lines = new ArrayList<>();
        for (int i = 0; i < Math.min(end - start, countLines); i++) {
            lines.add(readline(start + i, allCols, selectedCols));
        }

        return lines;
    }

    // DEBUG
    public ArrayList<Object[]> getAllLines(ArrayList<Column> cols) {
        ArrayList<Object[]> linesList = new ArrayList<>();
        try {
            for (int i = 0; i < countLines; i++) {
                linesList.add(readline(i, cols, cols));
            }
            return linesList;
        } catch (Exception e) {
            e.printStackTrace();
            return linesList;
        }

    }
}
