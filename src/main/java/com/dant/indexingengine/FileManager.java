package com.dant.indexingengine;

import org.apache.commons.lang3.SerializationUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class FileManager {

    private ArrayList<long[]> lines;
    private RandomAccessFile randomAccessFile;

    public FileManager() throws FileNotFoundException {
        lines = new ArrayList<>();
        randomAccessFile = new RandomAccessFile("src/main/resources/csv/saved.bin", "rw");
    }

    public long writeLine(Object[] line) throws IOException {
        byte[] data = SerializationUtils.serialize(line);
        long startPos = getPosEndFile();
        if(startPos > 0) ++startPos;
        lines.add(new long[]{startPos, data.length});
        randomAccessFile.seek(startPos);
        randomAccessFile.write(data);
        return lines.size() - 1;
    }

    public Object[] readline(int no) throws IOException {
        long[] linePos = lines.get(no);
        randomAccessFile.seek(linePos[0]);
        byte[] serializedLine = new byte[(int) linePos[1]];
        randomAccessFile.read(serializedLine, 0, (int) linePos[1]);
        return SerializationUtils.deserialize(serializedLine);
    }

    private long getPosEndFile() {
        if(lines.size() == 0) return 0;
        return lines.get(lines.size() - 1)[0] + lines.get(lines.size() - 1)[1];
    }

    public int countLine() { return lines.size(); }

    public long size() throws IOException { return randomAccessFile.length(); }

    // DEBUG
    public ArrayList<Object[]> getAllLines() {
        ArrayList<Object[]> linesList = new ArrayList<>();
        System.out.println(lines.size());
        try{
            for(int i = 0; i < (int) lines.size(); i++) {
                linesList.add(readline(i));
            }
            return linesList;
        } catch (Exception e) {
            e.printStackTrace();
            return linesList;
        }

    }
}
