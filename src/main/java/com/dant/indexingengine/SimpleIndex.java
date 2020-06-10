package com.dant.indexingengine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SimpleIndex {

    private String colName;
    private final HashMap<Object, IndexHolder> indexedData;

    public SimpleIndex() {
        indexedData = new HashMap<>();
    }

    public void index(Object o, int noLine) throws IOException {
        indexedData.computeIfAbsent(o, k -> {
            try {
                return new IndexHolder();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        });
        indexedData.get(o).addLine(noLine);
    }

    public List<Integer> get(Object o, int limit) throws IOException {
        return indexedData.containsKey(o) ? indexedData.get(o).getNumberOfLines(limit) : new ArrayList<>();
    }
}

class IndexHolder {

    private final ArrayList<Integer> linesNumberBuffer;
    private final RandomAccessFile saveFile;
    public int totalLinesInserted;
    public final int MAX_BUFFERED_LINES = 10000;
    public final int BYTE_INT_SIZE = 4;


    IndexHolder() throws FileNotFoundException {
        linesNumberBuffer = new ArrayList<>();
        saveFile = new RandomAccessFile(IndexingEngineSingleton.getInstance().getNewFilePath(), "rw");
        totalLinesInserted = 0;
    }

    public void addLine(int noLine) throws IOException {
        //TODO : exception
        if (linesNumberBuffer.size() < MAX_BUFFERED_LINES) {
            linesNumberBuffer.add(noLine);
        } else {
            saveFile.seek(saveFile.length());
            saveFile.writeInt(noLine);
        }
        totalLinesInserted++;
    }

    public List<Integer> getNumberOfLines(int lineCount) throws IOException {
        // TODO : exception
        if (lineCount <= linesNumberBuffer.size()) {
            return linesNumberBuffer.subList(0, lineCount);
        }
        ArrayList<Integer> tmpList = new ArrayList<>(linesNumberBuffer);

        int limit = lineCount > totalLinesInserted ? totalLinesInserted - tmpList.size() : lineCount - tmpList.size();

        saveFile.seek(0);
        for (int i = 0; i < limit; i++) {
            tmpList.add(saveFile.readInt());
        }

        return tmpList;
    }

}
