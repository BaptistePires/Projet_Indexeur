package com.dant.indexingengine.indexes;

import com.dant.exception.UnsupportedTypeException;
import com.dant.indexingengine.columns.Column;
import com.dant.indexingengine.columns.DoubleColumn;
import com.dant.indexingengine.columns.IntegerColumn;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public class NumberIndex extends BasicIndex {

    private final ArrayList<IndexContainer> indexedLines;
    private boolean sorted;
    Comparator<IndexContainer> comparator;

    public NumberIndex(Column c) throws UnsupportedTypeException {
        indexedLines = new ArrayList<>();
        sorted = false;
        comparator = getComparator(c);
    }

    @Override
    public void indexObject(Object o, int noLine) throws IOException {
        int pos;
        pos = indexedLines.indexOf(o);
        if (pos == -1) {
            IndexContainer indexContainer = new IndexContainer((Number) o);
            indexedLines.add(indexContainer);
            pos = indexedLines.size() - 1;
        }
        System.out.println(pos);
        indexedLines.get(pos).lines.addLine(noLine);
        sorted = false;
    }

    public void sort() {
        indexedLines.sort(comparator);
        sorted = true;
    }

    @Override
    public List<Integer> findLinesForObjectEquals(Object o, int limit) throws IOException {
        if (!sorted) sort();
        int low = 0, high = indexedLines.size();
        int mid;
        int index = -1;
        double key = Double.parseDouble(o.toString());

        List<Integer> returnedList = null;
        while (low <= high) {
            mid = (low + high) / 2;
            if (key == indexedLines.get(mid).value.doubleValue()) {
                index = mid;
                break;
            } else if (key < indexedLines.get(mid).value.doubleValue()) {
                high = mid - 1;
            } else if (key > indexedLines.get(mid).value.doubleValue()) {
                low = mid + 1;
            }
        }

        if (index > 0) returnedList = indexedLines.get(index).lines.getNumberOfLines(limit);

        return returnedList;
    }

    @Override
    public List<Integer> findLinesForObjectSuperior(Object o, int limit) throws IOException {
        return null;
    }

    @Override
    public int size() {
        return indexedLines.size();
    }

    private Comparator<IndexContainer> getComparator(Column c) throws UnsupportedTypeException {
        if (c instanceof IntegerColumn) {
            return Comparator.comparing(o -> ((Integer) o.value));
        } else if (c instanceof DoubleColumn) {
            return Comparator.comparing(o -> ((Double) o.value));
        } else {
            throw new UnsupportedTypeException("[NumberIndex - getComparator] Column type provided is not supported for this index : " + c.getClass());
        }
    }

    private static class IndexContainer {

        public Number value;
        public IndexLinesHolder lines;

        public IndexContainer(Number i) throws FileNotFoundException {
            lines = new IndexLinesHolder();
            value = i;
        }

    }
}
