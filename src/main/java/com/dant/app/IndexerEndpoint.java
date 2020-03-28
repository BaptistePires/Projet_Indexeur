package com.dant.app;

import com.dant.entity.Column;
import com.dant.entity.Query;
import com.dant.exception.InvalidFileException;
import com.dant.exception.InvalidIndexException;
import com.dant.exception.UnsupportedTypeException;
import com.dant.indexing_engine.IndexingEngineSingleton;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jboss.resteasy.annotations.GZIP;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
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
public class IndexerEndpoint {

    IndexingEngineSingleton indexingEngine = IndexingEngineSingleton.getInstance();


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


    @GET
    @Path("/showTable")
    public Set<Column> showTable() {
        return IndexingEngineSingleton.getInstance().getTable().getColumns();
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


    @GET
    @Path("/showIndex")
    public Set<Column> showIndex() {
        return IndexingEngineSingleton.getInstance().getTable().getIndexedColumns();
    }


    @POST
    @Path("/uploadData")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadData(@FormDataParam("file") InputStream uploadedInputStream,
                               @FormDataParam("file") FormDataContentDisposition fileDetail)
            throws InvalidFileException {
        System.out.println("RECEIVED FILE " + fileDetail.getFileName());
        String location = Paths.get(".", "src", "main", "resources", "csv", fileDetail.getFileName()).toString();
        if (!fileDetail.getFileName().endsWith(".csv")) {
            throw new InvalidFileException(fileDetail.getFileName());
        } else {
            try {
                FileOutputStream out = new FileOutputStream(new File(location));
                int read = 0;
                byte[] bytes = new byte[1024];
                while ((read = uploadedInputStream.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Response.status(200).entity("File successfully uploaded to " + location).build();
    }

    @POST
    @Path("/startIndexing")
    public Response startIndexing() {
        if (!indexingEngine.canIndex()) {
            return Response.status(403).entity("IndexingEngine is not ready to process your data").build();
        }

        Thread t = new Thread() {

            @Override
            public void run() {
                super.run();
                indexingEngine.startIndexing();
            }
        };
        t.start();
        return Response.status(200).entity("Indexing stated").build();
    }

    /**
     *
     * @param q Query object, must be like :
     *
     * 	            {"type": "SELECT",
     * 	             "cols": ["VendorID"],
     * 		         "conditions": {
     *                  "VendorID": {
     * 				        "operator": "=",
     * 				        "value": 2
     *                        }
     *               }
     *               }
     *
     *
     *          Currently only support one index, need improvements.
     *          There is no columns selection too. All columns will be returned
     *          as String. TODO: fix type issue.
     */
    @POST
    @GZIP
    @Path("/query")
    public Response testQuery(Query q) {
        return Response.status(200).type(MediaType.APPLICATION_JSON_TYPE).entity(IndexingEngineSingleton.getInstance().handleQuery(q)).build();
    }

}
