package com.dant.app;

import com.dant.entity.Column;
import com.dant.entity.Query;
import com.dant.exception.InvalidFileException;
import com.dant.exception.InvalidIndexException;
import com.dant.exception.NoDataException;
import com.dant.exception.UnsupportedTypeException;
import com.dant.indexingengine.IndexingEngineSingleton;
import com.dant.indexingengine.QueryHandler;
import com.dant.utils.IndexerUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Slf4j
public class IndexerEndpoint {

    private IndexingEngineSingleton indexingEngine = IndexingEngineSingleton.getInstance();
    private QueryHandler queryHandler = QueryHandler.getInstance();
    private static String uploadedFileName = "";

    // POST
    @POST
    @Path("/createTable")
    public Response createTable(String body) throws UnsupportedTypeException {
        // TODO : Check duplicated columns
        JsonObject columns = new JsonParser().parse(body).getAsJsonObject();
        for (Map.Entry<String, JsonElement> col : columns.entrySet()) {
            IndexingEngineSingleton.getInstance().getTable().addColumn(new Column(col.getKey(), col.getValue().getAsString()));
        }
        return Response.status(201).build();
    }


    /**
     * Method that create the indexes for a table.
     *
     * @param indexesToAdd List that contains names of columns that need be indexed
     * @return {@link Response}
     * @throws {@link InvalidIndexException} InvalidIndexException If an index provided does not exist.
     */
    @POST
    @Path("/addIndexes")
    public Response addIndex(List<String> indexesToAdd) throws InvalidIndexException {
        // Before inserting indexes, we must check data integrity
        if (indexesToAdd.size() > IndexingEngineSingleton.getInstance().getTable().getColumns().size())
            throw new InvalidIndexException("You provided more indexes" +
                    "than there are columns.");
        List<String> allColumnsName = IndexingEngineSingleton.getInstance().getTable().getColumnsName();
        if (!allColumnsName.containsAll(indexesToAdd)) {
            List<String> invalidIndexes = new ArrayList<>();
            for (String s : indexesToAdd) {
                if (!allColumnsName.contains(s)) invalidIndexes.add(s);
            }
            throw new InvalidIndexException(invalidIndexes.toString());
        }
        // Add indexes
        for (String s : indexesToAdd) {
            IndexingEngineSingleton.getInstance().getTable().addIndexByName(s);
        }
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
                    uploadedFileName = location;

		            // saving
		            IndexerUtil.saveFile(bytes, location);
	            }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.info("Uploaded file to " + location);
        return Response.status(200).entity("Uploaded file to : " + location).build();
    }

    @POST
    @Path("/startIndexing")
    public Response startIndexing() {
        if (!indexingEngine.canIndex()) {
            return Response.status(403).entity("IndexingEngine is not ready to process your data").build();
        }

        Thread t = new Thread() {

            @SneakyThrows
            @Override
            public void run() {
                super.run();
                log.info("Indexing started");
                indexingEngine.startIndexing(uploadedFileName);
                log.info("Finished indexing file " + uploadedFileName);
            }
        };
        t.start();
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
    	log.info("Received " + q.toString());
        return Response.status(200).type(MediaType.APPLICATION_JSON_TYPE).entity(queryHandler.handleQuery(q)).build();
    }


    // GET
	@GET
	@Path("/showTable")
	public Set<Column> showTable() {
		return IndexingEngineSingleton.getInstance().getTable().getColumns();
	}

	@GET
	@Path("/showIndex")
	public Set<Column> showIndex() {
		return IndexingEngineSingleton.getInstance().getTable().getIndexedColumns();
	}

	@GET
	@Path("/getState")
	public Response getState() {
		String canIndex = indexingEngine.canIndex() ? "YES" : "NO";
		String canQuery = indexingEngine.canQuery() ? "YES" : "NO";
		return Response.status(200)
				.entity("State :\n\tCan Index : " + canIndex + "\n\tCan query : " + canQuery)
				.build();
	}

}
