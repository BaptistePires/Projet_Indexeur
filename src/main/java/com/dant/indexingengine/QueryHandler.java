package com.dant.indexingengine;

import com.dant.entity.Query;
import com.dant.exception.NoDataException;
import com.google.gson.JsonObject;

import java.io.RandomAccessFile;
import java.util.*;

public class QueryHandler {

	private static final QueryHandler INSTANCE;

	private IndexingEngineSingleton indexer = IndexingEngineSingleton.getInstance();

	private QueryHandler() {}

	static {
		INSTANCE = new QueryHandler();
	}

	public static QueryHandler getInstance() {
		return INSTANCE;
	}

	/**
	 * For handling a query on the .csv
	 * @param q : Query object
	 * @return {@link List <JsonObject>} : Query results
	 */
	public List<JsonObject> handleQuery(Query q) throws NoDataException {
		RandomAccessFile randomAccessFile = indexer.getRandomAccessFile();
		JsonObject jsonObject;
		try {
			List<JsonObject> returnedData = new ArrayList<>();
			if (q.getType().equalsIgnoreCase("select")) {

				HashMap<String, Object> tmpIndex = new HashMap<>();
				for (Map.Entry<String, Map<String, Object>> entry : q.getConditions().entrySet()) {

					if (entry.getValue().get("operator").equals("=")) {
						tmpIndex.put(entry.getKey(), indexer.getTable().getColumnByName(entry.getKey()).castStringToType(entry.getValue().get("value").toString()));
					}
				}
				List<Integer> lineNumbers = indexer.getIndexedData().get(tmpIndex);

				for (int lineNumber : lineNumbers) {
					randomAccessFile.seek(indexer.getOffsets().get(lineNumber - 1));
					String[] splitLine = randomAccessFile.readLine().split(",");
					jsonObject = new JsonObject();
					for (int i = 0; i < splitLine.length; i++) {
						jsonObject.addProperty(
								indexer.getTable().getColumnByNo(i).getName(),
								indexer.getGson().toJson(
										indexer.getTable().getColumnByNo(i)
												.castStringToType(splitLine[i]),
										indexer.getTable().getColumnByNo(i).getType()
								)
						);
					}
					returnedData.add(jsonObject);
				}

				return returnedData;
			}
		} catch (Exception e) {
			throw new NoDataException();
		}
		return Collections.emptyList();
	}

	/**
	 * For testing, returns line numbers of query results, not actual data
	 */
	public List<Integer> getResultAsLineNumbers(Query q) throws NoDataException {
		List<Integer> lineNumbers = new ArrayList<>();

		try {
			if (q.getType().equalsIgnoreCase("select")) {
				HashMap<String, Object> tmpIndex = new HashMap<>();
				for (Map.Entry<String, Map<String, Object>> entry : q.getConditions().entrySet()) {

					if (entry.getValue().get("operator").equals("=")) {
						tmpIndex.put(entry.getKey(), indexer.getTable().getColumnByName(entry.getKey()).castStringToType(entry.getValue().get("value").toString()));
					}
				}
				lineNumbers = indexer.getIndexedData().get(tmpIndex);
			}
		} catch (Exception e) {
			throw new NoDataException();
		}

		return lineNumbers;
	}
}