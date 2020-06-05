package com.dant.indexingengine;

import com.dant.indexingengine.Column;
import com.dant.indexingengine.Query;
import com.dant.exception.NoDataException;
import com.dant.exception.UnsupportedTypeException;
import com.google.gson.JsonObject;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexingEngineTest {
//
//	static QueryHandler queryHandler = QueryHandler.getInstance();
//
//	static IndexingEngineSingleton indexingEngineSingleton = IndexingEngineSingleton.getInstance();
//
//	private static String location = Paths.get(".", "src", "main", "resources", "csv", "unit_tests.csv")
//			.toString();
//
//	@BeforeAll
//	static void setUp() throws UnsupportedTypeException, IOException {
//		indexingEngineSingleton.getTable().addColumn(new Column("VendorID", "Integer"));
//		indexingEngineSingleton.getTable().addColumn(new Column("tpep_pickup_datetime", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("tpep_dropoff_datetime", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("passenger_count", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("trip_distance", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("RatecodeID", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("store_and_fwd_flag", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("PULocationID", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("DOLocationID", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("payment_type", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("fare_amount", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("extra", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("mta_tax", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("tip_amount", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("tolls_amount", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("improvement_surcharge", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("total_amount", "String"));
//		indexingEngineSingleton.getTable().addColumn(new Column("congestion_surcharge", "String"));
//
//		indexingEngineSingleton.getTable().addIndexByName("VendorID");
//
//		indexingEngineSingleton.startIndexing(location);
//	}
//
//	@Test
//	void should_return_json_object_list() {
//		// GIVEN
//		List<String> cols = new ArrayList<>();
//		cols.add("VendorID");
//
//		Map<String, Map<String, Object>> conditions = new HashMap<>();
//		Map<String, Object> operation = new HashMap<>();
//		operation.put("operator", "=");
//		operation.put("value", 2);
//		conditions.put("VendorID", operation);
//
//		Query q = new Query("SELECT", cols, conditions);
//
//		// WHEN
//		List<JsonObject> result;
//		try {
//			result = queryHandler.handleQuery(q);
//		} catch (Exception e) {
//			result = new ArrayList<>();
//		}
//
//		// THEN
//		assertEquals(8, result.size());
//	}
//
//	@Test()
//	void should_throw_no_data_exception() {
//		// GIVEN
//		List<String> cols = new ArrayList<>();
//		cols.add("VendorID");
//
//		Map<String, Map<String, Object>> conditions = new HashMap<>();
//		Map<String, Object> operation = new HashMap<>();
//		operation.put("operator", "=");
//		operation.put("value", 5);
//		conditions.put("VendorID", operation);
//
//		Query q = new Query("SELECT", cols, conditions);
//
//		// WHEN, THEN
//		assertThrows(NoDataException.class, () -> {
//			List<JsonObject> result = queryHandler.handleQuery(q);
//		});
//	}

}
