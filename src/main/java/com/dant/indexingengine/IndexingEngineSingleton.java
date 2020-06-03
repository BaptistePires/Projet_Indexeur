package com.dant.indexingengine;

import com.google.gson.Gson;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.Data;
import org.apache.commons.lang3.SerializationUtils;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.*;

// TODO : Tmp -> improve after POC (Move to another class handling data) + Parse ALL colums, not just indexes
// Will work with only one index currently
@Data
public class IndexingEngineSingleton {

    private static final IndexingEngineSingleton INSTANCE;

    private ArrayList<Table> tables;
	private ArrayList<int[]> data;

	private Gson gson = new Gson();
	private String filePath;

    private boolean indexed;
    private boolean indexing;
    private boolean error;

    static {
        INSTANCE = new IndexingEngineSingleton();
    }

    private IndexingEngineSingleton() {
        tables = new ArrayList<>();
        indexed = false;
        indexing = false;
        data = new ArrayList<>();
    }

    public static IndexingEngineSingleton getInstance() {
        return INSTANCE;
    }

	/**
	 * Indexes the file and keeps line offsets for future queries
	 * @param filePath path to .csv file;
	 * @throws {@link IOException}
	 */
	public void startIndexing(String filePath, String tableName) throws IOException {
		// Files related vars
		String fileName = "src/main/resources/csv/test.csv";
		FileInputStream fis = new FileInputStream(fileName);
		InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
		CSVReader reader = new CSVReader(isr);
		ByteArrayOutputStream  bos = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(bos);

		// Reading CSV vars
		String[] lineArray;
		String line;
		Object[] castedLine;
		int i, headerLength;

		Table t;
		if((t = getTableByName(tableName)) == null) return;

		// Saving vars
		String savePath = "src/main/resources/csv/saved.bin";
		int[] linePositions;
		byte[] dataBytes;
		int lineCounter = 0;
		RandomAccessFile saveFile = new RandomAccessFile(savePath, "rw");


		try {
			lineArray = reader.readNext();
			headerLength = lineArray.length;
			if(lineArray.length != getTableByName(tableName).getColumns().size()) {
				System.out.println("Error, the file provided does not correspond to the table;");
				return;
			}

			// Setting up columns No
			for(i = 0; i < headerLength; i++) {
				t.getColumnByName(lineArray[i]).setColumnNo(i);
			}
			t.mapColumnsByNo();


			while((lineArray = reader.readNext()) != null) {

				// Cast data
				castedLine = new Object[headerLength];
				for(i = 0; i < headerLength; i++) {
					castedLine[i] = t.getColumnByNo(i).castAndUpdateMetaData(lineArray[i]);
				}

				// Serialize data
				dataBytes = SerializationUtils.serialize(castedLine);
				linePositions = new int[2];
				if(lineCounter != 0) {
					linePositions[0] = data.get(lineCounter - 1)[0] + data.get(lineCounter -1 )[1];
				}
				linePositions[1] = dataBytes.length;
				data.add(linePositions);
				saveFile.write(dataBytes);
				lineCounter++;
			}

			// Debug, will be removed
			byte[] read;
			for(i = 0; i < lineCounter; i++) {
				saveFile.seek(data.get(i)[0]);
				read = new byte[data.get(i)[1]];
				saveFile.read(read, 0, data.get(i)[1]);
				Object[] o = SerializationUtils.deserialize(read);
//				System.out.println(Arrays.toString(o));
			}

		} catch (CsvValidationException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// ->>> t.mapColumnByNo; handle exception better
			e.printStackTrace();
		}
		finally {
			objectOutputStream.close();
			saveFile.close();

		}

    }

	public ArrayList<Table> getTables() {
		return tables;
	}

	public Table getTableByName(String name)  {
	    for(Table t: tables) {
	        if(t.getName().equals(name)){
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

}


