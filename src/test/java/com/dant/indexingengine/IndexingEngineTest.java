package com.dant.indexingengine;

import com.dant.exception.TableNotFoundException;
import com.dant.exception.UnsupportedTypeException;
import com.dant.exception.WrongFileFormatException;
import com.dant.indexingengine.columns.DoubleColumn;
import com.dant.indexingengine.columns.IntegerColumn;
import com.dant.indexingengine.columns.StringColumn;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexingEngineTest {

	static QueryHandler queryHandler = QueryHandler.getInstance();

	static IndexingEngineSingleton indexingEngineSingleton = IndexingEngineSingleton.getInstance();

	private static String[] columnNames = {"VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
			"passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
			"payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
			"total_amount", "congestion_surcharge"};

	private static final String TABLE_NAME = "nyc_cab";
	private static final String INDEXED_COL_NAME_1 = columnNames[0];
	private static final String INDEXED_COL_NAME_2 = columnNames[3];

	private static final String AND = "AND";
	private static final String OR = "OR";

	private static File uploadsFolder = Paths.get("src", "main", "resources", "uploads").toFile();

	@BeforeAll
	static void setUp() throws UnsupportedTypeException, TableNotFoundException, IOException, WrongFileFormatException {

		if(!uploadsFolder.exists()){
			uploadsFolder.mkdirs();
		}else{
			for(String file: Objects.requireNonNull(uploadsFolder.list())) {
				File f = Paths.get("src", "main", "resources", "uploads", file).toFile();
				f.delete();
			}
		}

		// Copying test file
		File source = Paths.get("src", "main", "resources", "test", "unit_test.csv").toFile();
		File dest = Paths.get("src", "main", "resources", "uploads", "unit_test.csv").toFile();
		Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);

		// Setting up indexer
		Table table = new Table(TABLE_NAME);

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
		table.addColumn(new IntegerColumn(columnNames[17]));

		indexingEngineSingleton.addTable(table);

		indexingEngineSingleton.getTableByName(TABLE_NAME).getColumnByName(INDEXED_COL_NAME_1).setIndexed();
		indexingEngineSingleton.getTableByName(TABLE_NAME).getColumnByName(INDEXED_COL_NAME_2).setIndexed();

		indexingEngineSingleton.startIndexing(TABLE_NAME);
	}

	@AfterAll
	static void endUp() {
		for(String file: Objects.requireNonNull(uploadsFolder.list())) {
			File f = Paths.get("src", "main", "resources", "uploads", file).toFile();
			f.delete();
		}
	}

	@Test
	void should_return_json_object_list_with_one_condition() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add(columnNames[0]);

		Map<String, Map<String, Object>> conditions = new HashMap<>();
		Map<String, Object> operation = new HashMap<>();
		operation.put("operator", "=");
		operation.put("value", 2.0);
		conditions.put(columnNames[0], operation);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME, AND);

		// WHEN
		JsonObject result;
		try {
			result = queryHandler.handleQuery(q);
		} catch (Exception e) {
			e.printStackTrace();
			result = new JsonObject();
		}

		// THEN
		assertEquals(8, result.getAsJsonArray("lines").size());
	}

	@Test
	void should_return_json_object_list_with_more_conditions_AND() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add(columnNames[0]);
		cols.add(columnNames[3]);

		Map<String, Map<String, Object>> conditions = new HashMap<>();

		// Condition 1
		Map<String, Object> operation1 = new HashMap<>();
		operation1.put("operator", "=");
		operation1.put("value", 1);

		// Condition 2
		Map<String, Object> operation2 = new HashMap<>();
		operation2.put("operator", "=");
		operation2.put("value", 1);

		conditions.put(columnNames[0], operation1);
		conditions.put(columnNames[3], operation2);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME, AND);

		// WHEN
		JsonObject result;
		try {
			result = queryHandler.handleQuery(q);
		} catch (Exception e) {
			e.printStackTrace();
			result = new JsonObject();
		}

		// THEN
		assertEquals(8, result.getAsJsonArray("lines").size());
	}

	@Test
	void should_return_json_object_list_with_more_conditions_OR() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add(columnNames[0]);
		cols.add(columnNames[3]);

		Map<String, Map<String, Object>> conditions = new HashMap<>();

		// Condition 1
		Map<String, Object> operation1 = new HashMap<>();
		operation1.put("operator", "=");
		operation1.put("value", 2);

		// Condition 2
		Map<String, Object> operation2 = new HashMap<>();
		operation2.put("operator", "=");
		operation2.put("value", 2);

		conditions.put(columnNames[0], operation1);
		conditions.put(columnNames[3], operation2);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME, OR);

		// WHEN
		JsonObject result;
		try {
			result = queryHandler.handleQuery(q);
		} catch (Exception e) {
			e.printStackTrace();
			result = new JsonObject();
		}

		System.out.println(result.getAsJsonArray("lines"));

		// THEN
		assertEquals(10, result.getAsJsonArray("lines").size());
	}

	@Test()
	void should_return_0_results_simple_query() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add(columnNames[0]);

		Map<String, Map<String, Object>> conditions = new HashMap<>();
		Map<String, Object> operation = new HashMap<>();
		operation.put("operator", "=");
		operation.put("value", 5);
		conditions.put(columnNames[0], operation);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME, AND);

		// WHEN
		JsonObject result;
		try {
			result = queryHandler.handleQuery(q);
		} catch (Exception e) {
			e.printStackTrace();
			result = new JsonObject();
		}

		// THEN
		assertEquals(0, result.getAsJsonArray("lines").size());
	}

	@Test
	void should_return_0_results_multiple_AND_query() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add(columnNames[0]);
		cols.add(columnNames[3]);

		Map<String, Map<String, Object>> conditions = new HashMap<>();

		// Condition 1
		Map<String, Object> operation1 = new HashMap<>();
		operation1.put("operator", "=");
		operation1.put("value", 2);

		// Condition 2
		Map<String, Object> operation2 = new HashMap<>();
		operation2.put("operator", "=");
		operation2.put("value", 2);

		conditions.put(columnNames[0], operation1);
		conditions.put(columnNames[3], operation2);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME, AND);

		// WHEN
		JsonObject result;
		try {
			result = queryHandler.handleQuery(q);
		} catch (Exception e) {
			e.printStackTrace();
			result = new JsonObject();
		}

		// THEN
		assertEquals(0, result.getAsJsonArray("lines").size());
	}

	@Test
	void should_return_0_results_multiple_OR_query() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add(columnNames[0]);
		cols.add(columnNames[3]);

		Map<String, Map<String, Object>> conditions = new HashMap<>();

		// Condition 1
		Map<String, Object> operation1 = new HashMap<>();
		operation1.put("operator", "=");
		operation1.put("value", 3);

		// Condition 2
		Map<String, Object> operation2 = new HashMap<>();
		operation2.put("operator", "=");
		operation2.put("value", 6);

		conditions.put(columnNames[0], operation1);
		conditions.put(columnNames[3], operation2);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME, OR);

		// WHEN
		JsonObject result;
		try {
			result = queryHandler.handleQuery(q);
		} catch (Exception e) {
			e.printStackTrace();
			result = new JsonObject();
		}

		// THEN
		assertEquals(0, result.getAsJsonArray("lines").size());
	}



}
