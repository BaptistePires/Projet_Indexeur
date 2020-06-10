package com.dant.indexingengine;

import com.dant.exception.NoDataException;
import com.dant.exception.TableNotFoundException;
import com.dant.exception.UnsupportedTypeException;
import com.dant.indexingengine.columns.DoubleColumn;
import com.dant.indexingengine.columns.IntegerColumn;
import com.dant.indexingengine.columns.StringColumn;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexingEngineTest {

	static QueryHandler queryHandler = QueryHandler.getInstance();

	static IndexingEngineSingleton indexingEngineSingleton = IndexingEngineSingleton.getInstance();

	private static final String TEST_FILE_LOCATION
			= Paths.get(".", "src", "main", "resources", "uploads", "unit_tests.csv").toString();

	private static final String TABLE_NAME = "nyc_cab";
	private static final String INDEXED_COL_NAME = "VendorID";

	@BeforeAll
	static void setUp() throws UnsupportedTypeException, TableNotFoundException, IOException {
		Table table = new Table(TABLE_NAME);

		table.addColumn(new IntegerColumn("VendorID"));
		table.addColumn(new StringColumn("tpep_pickup_datetime"));
		table.addColumn(new StringColumn("tpep_dropoff_datetime"));
		table.addColumn(new IntegerColumn("passenger_count"));
		table.addColumn(new DoubleColumn("trip_distance"));
		table.addColumn(new StringColumn("RatecodeID"));
		table.addColumn(new StringColumn("store_and_fwd_flag"));
		table.addColumn(new IntegerColumn("PULocationID"));
		table.addColumn(new IntegerColumn("DOLocationID"));
		table.addColumn(new IntegerColumn("payment_type"));
		table.addColumn(new DoubleColumn("fare_amount"));
		table.addColumn(new DoubleColumn("extra"));
		table.addColumn(new DoubleColumn("mta_tax"));
		table.addColumn(new DoubleColumn("tip_amount"));
		table.addColumn(new DoubleColumn("tolls_amount"));
		table.addColumn(new DoubleColumn("improvement_surcharge"));
		table.addColumn(new DoubleColumn("total_amount"));
		table.addColumn(new DoubleColumn("congestion_surcharge"));

		indexingEngineSingleton.addTable(table);

		indexingEngineSingleton.getTableByName(TABLE_NAME).getColumnByName(INDEXED_COL_NAME).setIndexed();

		indexingEngineSingleton.startIndexing(TEST_FILE_LOCATION, TABLE_NAME);
	}

	@Test
	void should_return_json_object_list() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add("VendorID");

		Map<String, Map<String, Object>> conditions = new HashMap<>();
		Map<String, Object> operation = new HashMap<>();
		operation.put("operator", "=");
		operation.put("value", 2.0);
		conditions.put("VendorID", operation);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME);

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

	@Test()
	void should_throw_no_data_exception() {
		// GIVEN
		List<String> cols = new ArrayList<>();
		cols.add("VendorID");

		Map<String, Map<String, Object>> conditions = new HashMap<>();
		Map<String, Object> operation = new HashMap<>();
		operation.put("operator", "=");
		operation.put("value", 5);
		conditions.put("VendorID", operation);

		Query q = new Query("SELECT", cols, conditions, 100, TABLE_NAME);

		// WHEN, THEN
		assertThrows(NoDataException.class, () -> {
			JsonObject result = queryHandler.handleQuery(q);
		});
	}

}
