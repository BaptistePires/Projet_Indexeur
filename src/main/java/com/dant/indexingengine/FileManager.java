package com.dant.indexingengine;

import org.apache.commons.lang3.SerializationUtils;

import java.io.*;
import java.util.ArrayList;

public class FileManager {


    private long countLines;
    private RandomAccessFile dataSavedToDisk;
    private RandomAccessFile linesPositionsInFile;
    private final int BYTE_SIZE_LONG_ARRAY = 43;

    public FileManager() throws FileNotFoundException {
        dataSavedToDisk = new RandomAccessFile("src/main/resources/csv/saved.bin", "rw");
        linesPositionsInFile = new RandomAccessFile("src/main/resources/csv/linesPositions.bin", "rw");

    }

    public long writeLine(Object[] line) throws IOException {
        byte[] data = SerializationUtils.serialize(line);
        long startPos = dataSavedToDisk.length();
        if(startPos > 0) ++startPos;
        savePositionsToFile(new long[]{startPos, data.length});
        dataSavedToDisk.seek(startPos);
        dataSavedToDisk.write(data);
        countLines++;
        return countLines - 1;
    }

    public Object[] readline(int no) throws IOException {
        long[] linePos = readPositionOfLine(no);
        dataSavedToDisk.seek(linePos[0]);
        byte[] serializedLine = new byte[(int) linePos[1]];
        dataSavedToDisk.read(serializedLine, 0, (int) linePos[1]);
        return SerializationUtils.deserialize(serializedLine);
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
        if(noLine > 1) startPosition++;
        byte[] positionArrayBytes = new byte[BYTE_SIZE_LONG_ARRAY];
        linesPositionsInFile.seek(startPosition);
        linesPositionsInFile.read(positionArrayBytes, 0, BYTE_SIZE_LONG_ARRAY);
        return SerializationUtils.deserialize(positionArrayBytes);
    }

    public int countLine() { return (int) countLines; }

    public long size() throws IOException { return dataSavedToDisk.length(); }








    // DEBUG
    public ArrayList<Object[]> getAllLines() {
        ArrayList<Object[]> linesList = new ArrayList<>();
        try{
            for(int i = 0; i < countLines; i++) {
                linesList.add(readline(i));
            }
            return linesList;
        } catch (Exception e) {
            e.printStackTrace();
            return linesList;
        }

    }
}