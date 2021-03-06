package com.dant.indexingengine.indexes;

import com.dant.indexingengine.IndexingEngineSingleton;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

class IndexLinesHolder {

    public final int MAX_BUFFERED_LINES = 10000;
    private final ArrayList<Integer> linesNumberBuffer;
    private final RandomAccessFile saveFile;
    public int totalLinesInserted;


    IndexLinesHolder() throws FileNotFoundException {
        linesNumberBuffer = new ArrayList<>();
        saveFile = new RandomAccessFile(IndexingEngineSingleton.getInstance().getNewFilePath(), "rw");
        totalLinesInserted = 0;
    }

    public void addLine(int noLine) throws IOException {
        if (linesNumberBuffer.size() < MAX_BUFFERED_LINES) {
            linesNumberBuffer.add(noLine);
        } else {
            saveFile.seek(saveFile.length());
            saveFile.writeInt(noLine);
        }
        totalLinesInserted++;
    }

    public List<Integer> getNumberOfLines(int lineCount) throws IOException {
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
