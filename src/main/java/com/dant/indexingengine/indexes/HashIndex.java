package com.dant.indexingengine.indexes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * This class will be used as an Index based on a HashMap.
 */
public class HashIndex extends BasicIndex {

    private HashMap<Object, IndexLinesOlder> indexedLines;

    public HashIndex() {
        indexedLines = new HashMap<>();
    }

    @Override
    public void indexObject(Object o, int noLine) throws IOException {
        indexedLines.computeIfAbsent(o, k -> {
            try {
                return new IndexLinesOlder();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        });
        indexedLines.get(o).addLine(noLine);
    }

    @Override
    public List<Integer> findLinesForObject(Object o, int limit) throws IOException {
        return indexedLines.containsKey(o) ? indexedLines.get(o).getNumberOfLines(limit) : new ArrayList<>();
    }
}
