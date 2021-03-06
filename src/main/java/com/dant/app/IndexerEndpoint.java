package com.dant.app;

import com.dant.exception.*;
import com.dant.indexingengine.IndexingEngineSingleton;
import com.dant.indexingengine.Query;
import com.dant.indexingengine.QueryHandler;
import com.dant.indexingengine.Table;
import com.dant.indexingengine.columns.Column;
import com.dant.indexingengine.columns.DoubleColumn;
import com.dant.indexingengine.columns.IntegerColumn;
import com.dant.indexingengine.columns.StringColumn;
import com.dant.utils.IndexerUtil;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
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

    private final IndexingEngineSingleton indexingEngine = IndexingEngineSingleton.getInstance();
    private final QueryHandler queryHandler = QueryHandler.getInstance();
    private static String uploadedFilePath = "";
    private static String lastCreatedTableName = "";

    // POST
    @POST
    @Path("/createTable")
    public Response createTable(String body) throws UnsupportedTypeException {
        JsonObject tableLayout = new JsonParser().parse(body).getAsJsonObject();

        // Setting up name
        String tableName;
        try {
            tableName = tableLayout.get("name").getAsString();
        } catch (Exception e) {
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

                case "double":
                    c = new DoubleColumn(column.get("name").getAsString());
                    break;

                default:
                    c = new StringColumn(column.get("name").getAsString());
            }
            table.addColumn(c);
        }
        indexingEngine.addTable(table);
        lastCreatedTableName = tableName;

        return Response.status(201).entity(table).build();
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
        for (JsonElement e : indexedColumn) {
            t.getColumnByName(e.getAsString()).setIndexed();
        }

        return Response.status(201).entity(t).build();
    }


    @POST
    @Path("/uploadData")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadData(MultipartFormDataInput input) throws InvalidFileException {
        String location = "", fileName = "";
        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
        List<InputPart> inputParts = uploadForm.get("file");

        for (InputPart inputPart : inputParts) {
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
                    location = Paths.get(".", "src", "main", "resources", "uploads", fileName).toString();
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
    public Response startIndexing() throws IOException, TableNotFoundException, WrongFileFormatException {
        indexingEngine.startIndexing(lastCreatedTableName);
        return Response.status(200).entity("Indexing started").build();
    }


    /**
     * @param q Query object, must be like :
     *          <p>
     *          {
     *          "type": "SELECT",
     *          "cols": ["VendorID"],
     *          "conditions": {
     *          "VendorID": {
     *          "operator": "=",
     *          "value": 2
     *          }
     *          }
     *          }
     *          <p>
     *          <p>
     *          Currently only support one index, need improvements.
     *          There is no columns selection too. All columns will be returned
     *          as String.
     */
    @POST
    @GZIP
    @Path("/query")
    public Response testQuery(Query q) throws Exception {
        System.out.println("[INFO] Query received " + q.toString());
        return Response
                .status(200)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(queryHandler.handleQuery(q))
                .build();
    }

}
