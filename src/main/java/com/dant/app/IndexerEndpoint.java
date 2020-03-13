package com.dant.app;

import com.dant.entity.Column;
import com.dant.entity.Table;
import com.dant.exception.InvalidFileException;
import com.dant.exception.InvalidIndexException;
import com.dant.exception.UnsupportedTypeException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

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

    private Table table;


    @POST
    @Path("/createTable")
    public Response createTable(String body) throws UnsupportedTypeException {
        // TODO : Check duplicated columns
        JsonObject columns = new JsonParser().parse(body).getAsJsonObject();
        table = new Table();
        for (Map.Entry<String, JsonElement> col : columns.entrySet()) {
            table.addColumn(new Column(col.getKey(), col.getValue().getAsString()));
        }
        return Response.status(201).build();
    }


    @GET
    @Path("/showTable")
    public Table showTable() {
        return table;
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
        if (indexesToAdd.size() > table.getColumns().size())
            throw new InvalidIndexException("You provided more indexes" +
                    "than there are columns.");
        List<String> allColumnsName = table.getColumnsName();
        if (!allColumnsName.containsAll(indexesToAdd)) {
            List<String> invalidIndexes = new ArrayList<>();
            for (String s : indexesToAdd) {
                if (!allColumnsName.contains(s)) invalidIndexes.add(s);
            }
            throw new InvalidIndexException(invalidIndexes.toString());
        }
        // Add indexes
        for (String s : indexesToAdd) {
            table.addIndexByName(s);
        }
        return Response.status(201).build();
    }


    @GET
    @Path("/showIndex")
    public Set<Column> showIndex() {
        return this.table.getIndexedColumns();
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


    @GET
    @Path("/getRows")
    public void getRows() {
        // TODO
    }

}
