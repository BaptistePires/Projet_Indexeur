package com.dant.indexingengine;

import com.dant.exception.UnsupportedTypeException;
import com.dant.indexingengine.columns.DoubleColumn;
import com.dant.indexingengine.columns.IntegerColumn;
import com.dant.indexingengine.columns.StringColumn;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileManagerTest {

	private static final String TEST_FILE_LOCATION
			= Paths.get(".", "src", "main", "resources", "csv", "unit_tests.csv").toString();

	private static final String TABLE_NAME = "nyc_cab";

	private static Table table;

	private static String[] columnNames = {"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
			"passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
			"payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
			"total_amount", "congestion_surcharge"};

	FileManager fileManager;

	{
		try {
			fileManager = new FileManager();
		} catch (FileNotFoundException e) {}
	}

	@BeforeAll
	static void setUp() throws UnsupportedTypeException {
		table = new Table(TABLE_NAME);

		table.addColumn(new IntegerColumn(columnNames[0]));
		table.addColumn(new StringColumn(columnNames[1]));
		table.addColumn(new StringColumn(columnNames[2]));
		table.addColumn(new IntegerColumn(columnNames[3]));
		table.addColumn(new DoubleColumn(columnNames[4]));
		table.addColumn(new StringColumn(columnNames[5]));
		table.addColumn(new StringColumn(columnNames[6]));
		table.addColumn(new IntegerColumn(columnNames[7]));
		table.addColumn(new IntegerColumn(columnNames[8]));
		table.addColumn(new IntegerColumn(columnNames[9]));
		table.addColumn(new DoubleColumn(columnNames[10]));
		table.addColumn(new DoubleColumn(columnNames[11]));
		table.addColumn(new DoubleColumn(columnNames[12]));
		table.addColumn(new DoubleColumn(columnNames[13]));
		table.addColumn(new DoubleColumn(columnNames[14]));
		table.addColumn(new DoubleColumn(columnNames[15]));
		table.addColumn(new DoubleColumn(columnNames[16]));
		table.addColumn(new DoubleColumn(columnNames[17]));

		// Set columns N°s
		for (int i = 0; i < columnNames.length; i++) {
			table.getColumnByName(columnNames[i]).setColumnNo(i);
		}
	}

	@Test
	void should_write_and_read_full_line_from_csv() throws IOException, CsvValidationException {
		CSVReader csvReader = new CSVReader(
				new InputStreamReader(new FileInputStream(TEST_FILE_LOCATION), StandardCharsets.UTF_8)
		);

		String[] lineArray;
		Object[] castedLine, lineFromDisk;
		long lineNo;

		csvReader.readNext(); // Skip header
		lineArray = csvReader.readNext();

		castedLine = new Object[lineArray.length];
		for (int i = 0; i < lineArray.length; i++)
			castedLine[i] = table.getColumns().get(i).castAndUpdateMetaData(lineArray[i]);

		lineNo = fileManager.writeLine(castedLine, table.getColumns());

		lineFromDisk = fileManager.readline((int) lineNo, table.getColumns(), table.getColumns());

		for (int i = 0; i < castedLine.length; i++)
			assertEquals(castedLine[i], lineFromDisk[i]);
	}

	@Test
	void should_write_and_read_part_of_line_from_csv() throws IOException, CsvValidationException {
		CSVReader csvReader = new CSVReader(
				new InputStreamReader(new FileInputStream(TEST_FILE_LOCATION), StandardCharsets.UTF_8)
		);

		String[] lineArray;
		Object[] castedLine, lineFromDisk;
		long lineNo;

		List<String> wantedColumnsNames = new ArrayList<>();
		wantedColumnsNames.add(columnNames[0]);
		wantedColumnsNames.add(columnNames[1]);
		wantedColumnsNames.add(columnNames[7]);

		csvReader.readNext(); // Skip header
		lineArray = csvReader.readNext();

		castedLine = new Object[lineArray.length];
		for (int i = 0; i < lineArray.length; i++)
			castedLine[i] = table.getColumns().get(i).castAndUpdateMetaData(lineArray[i]);

		lineNo = fileManager.writeLine(castedLine, table.getColumns());

		lineFromDisk = fileManager.readline(
				(int) lineNo, table.getColumns(),
				table.getColumnsByNames(wantedColumnsNames)
		);

		for (int i = 0; i < lineFromDisk.length; i++)
			assertEquals(castedLine[table.getColumnByName(wantedColumnsNames.get(i)).getColumnNo()], lineFromDisk[i]);
	}

}
