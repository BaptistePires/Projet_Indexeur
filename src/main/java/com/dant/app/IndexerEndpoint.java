package com.dant.app;

import com.dant.indexingengine.*;
import com.dant.exception.InvalidFileException;
import com.dant.exception.InvalidIndexException;
import com.dant.exception.NoDataException;
import com.dant.exception.UnsupportedTypeException;
import com.dant.utils.IndexerUtil;
import com.google.gson.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IndexerEndpoint {

	private IndexingEngineSingleton indexingEngine = IndexingEngineSingleton.getInstance();
	private QueryHandler queryHandler = QueryHandler.getInstance();
	private static String uploadedFilePath = "";

	// POST
	@POST
	@Path("/createTable")
	public Response createTable(String body) throws UnsupportedTypeException {
		JsonObject tableLayout = new JsonParser().parse(body).getAsJsonObject();

		// Setting up name
		String tableName;
		try{
			tableName = tableLayout.get("name").getAsString();
		}catch (Exception e) {
			tableName = "default";
		}

		Table table = new Table(tableName);

		// Setting up columns
		Column c;
		for (JsonElement columnElement : tableLayout.getAsJsonArray("columns")) {
			JsonObject column = columnElement.getAsJsonObject();
			String type = column.get("type").getAsString().toLowerCase();
			switch (type) {
				case "integer":
					c = new IntegerColumn(column.get("name").getAsString());
					break;

				case "string":
					c = new StringColumn(column.get("name").getAsString());
					break;

				default:
					c = new StringColumn(column.get("name").getAsString());
			}
			table.addColumn(c);
		}
		indexingEngine.addTable(table);

		return Response.status(201).entity("Table \"" + tableName + "\" has been created").build();
	}


	/**
	 * Method that create the indexes for a table.
	 *
	 * @return {@link Response}
	 * @throws {@link InvalidIndexException} InvalidIndexException If an index provided does not exist.
	 */
	@POST
	@Path("/addIndexes")
	public Response addIndex(String body) throws Exception {
		JsonObject indexInfo = new JsonParser().parse(body).getAsJsonObject();
		Table t = indexingEngine.getTableByName(indexInfo.get("tableName").getAsString());
		JsonArray indexedColumn = indexInfo.get("indexes").getAsJsonArray();
		Column[] columns = new Column[indexedColumn.size()];
		for(int i = 0; i < indexedColumn.size(); i++) columns[i] = t.getColumnByName(indexedColumn.get(i).getAsString());
		t.addIndexByName(columns);
		// Before inserting indexes, we must check data integrity
//		if (indexesToAdd.size() > IndexingEngineSingleton.getInstance().getTable().getColumns().size())
//			throw new InvalidIndexException("You provided more indexes" +
//					"than there are columns.");
//		List<String> allColumnsName = IndexingEngineSingleton.getInstance().getTable().getColumnsName();
//		if (!allColumnsName.containsAll(indexesToAdd)) {
//			List<String> invalidIndexes = new ArrayList<>();
//			for (String s : indexesToAdd) {
//				if (!allColumnsName.contains(s)) invalidIndexes.add(s);
//			}
//			throw new InvalidIndexException(invalidIndexes.toString());
//		}
//		// Add indexes
//		for (String s : indexesToAdd) {
////			IndexingEngineSingleton.getInstance().getTable().addIndexByName(s);
//		}
		return Response.status(201).build();
	}


	@POST
	@Path("/uploadData")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadData(MultipartFormDataInput input) throws InvalidFileException {
        String location = "", fileName = "";
        Map <String, List <InputPart>> uploadForm = input.getFormDataMap();
        List <InputPart> inputParts = uploadForm.get("file");

        for (InputPart inputPart: inputParts) {
            try {
                MultivaluedMap<String, String> header = inputPart.getHeaders();
                fileName = IndexerUtil.getFileName(header);

	            if (!fileName.endsWith(".csv")) {
		            throw new InvalidFileException(fileName);
	            } else {
		            // File to InputStream
		            InputStream inputStream = inputPart.getBody(InputStream.class, null);
		            byte[] bytes = IOUtils.toByteArray(inputStream);

		            // to path
		            location = Paths.get(".", "src", "main", "resources", "csv", fileName).toString();
                    uploadedFilePath = location;

		            // saving
		            IndexerUtil.saveFile(bytes, location);
	            }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Uploaded file to " + location);
        return Response.status(200).entity("Uploaded file to : " + location).build();
	}



	@POST
	@Path("/startIndexing")
	public Response startIndexing() throws IOException {
//		if (!indexingEngine.canIndex()) {
//			return Response.status(403).entity("IndexingEngine is not ready to process your data").build();
//		}
//
//		Thread t = new Thread() {
//
//			@SneakyThrows
//			@Override
//			public void run() {
//				super.run();
//				log.info("Indexing started at "
//						+ DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss").format(LocalDateTime.now())
//				);
//				indexingEngine.startIndexing(uploadedFilePath);
//				log.info("Finished indexing file at "
//						+ DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss").format(LocalDateTime.now())
//				);
//			}
//		};
//		t.start();
		indexingEngine.startIndexing("", "TableName");
		return Response.status(200).entity("Indexing started").build();
	}



	/**
	 *
	 * @param q Query object, must be like :
	 *
	 * 	            {
	 * 	                "type": "SELECT",
	 * 	                "cols": ["VendorID"],
	 * 		            "conditions": {
	 *                      "VendorID": {
	 * 				            "operator": "=",
	 * 				            "value": 2
	 * 			            }
	 *                  }
	 *              }
	 *
	 *
	 *          Currently only support one index, need improvements.
	 *          There is no columns selection too. All columns will be returned
	 *          as String. TODO: fix type issue.
	 */
	@POST
	@GZIP
	@Path("/query")
	public Response testQuery(Query q) throws NoDataException {
//    	log.info("Received " + q.toString());
		return Response
				.status(200)
				.type(MediaType.APPLICATION_JSON_TYPE)
				.entity(queryHandler.handleQuery(q))
				.build();
	}
	
	// Debug
	@GET
	@Path("/allLines")
	public Response getAllLines() {
		return Response.status(200)
				.type(MediaType.APPLICATION_JSON_TYPE)
				.entity(indexingEngine.getallLines())
				.build();
	}


//	// GET
//	@GET
//	@Path("/showTable")
//	public Table showTable() {
//		return IndexingEngineSingleton.getInstance().getTable();
//	}
//
//	@GET
//	@Path("/showIndex")
//	public Set<Column> showIndex() {
//		return IndexingEngineSingleton.getInstance().getTable().getIndexedColumns();
//	}
//
//	@GET
//	@Path("/getState")
//	public Response getState() {
//		String canIndex = indexingEngine.canIndex() ? "YES" : "NO";
//		String canQuery = indexingEngine.canQuery() ? "YES" : "NO";
//		return Response.status(200)
//				.entity("State :\n\tCan Index : " + canIndex + "\n\tCan query : " + canQuery)
//				.build();
//	}

}
