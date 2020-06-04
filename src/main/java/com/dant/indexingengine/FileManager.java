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
        long[] pos = new long[2];
        pos[0] = getPosEndFile();
        pos[1] = data.length;
        lines.add(pos);
        randomAccessFile.seek(getPosEndFile());
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
}
