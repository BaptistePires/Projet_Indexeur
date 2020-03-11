package com.dant.app;

import com.dant.exception.InvalidFileException;
import com.dant.exception.InvalidIndexException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IndexerEndpoint {
	List<String> cols;
	List<String> indexes;


	@POST
	@Path("/createTable")
	public List<String> createTable(List<String> cols){
		System.out.println("Received columns " + cols);
		this.cols = cols;
		return this.cols;
	}


	@GET
	@Path("/showTable") // DEBUG
	public List<String> showTable(){
		return this.cols;
	}


	@POST
	@Path("/addIndex")
	public List<String> addIndex(List<String> indexesToAdd) throws InvalidIndexException {
		System.out.println("Received indexes to add: " + indexesToAdd);
		if(!this.cols.containsAll(indexesToAdd)){
			List<String> invalidIndexes = new ArrayList<>();
			for(String s: indexesToAdd){
				if(!this.cols.contains(s)){
					invalidIndexes.add(s);
				}
			}
			throw new InvalidIndexException(invalidIndexes.toString());
		}
		this.indexes = indexesToAdd;
		return this.indexes;
	}


	@GET
	@Path("/showIndex") // DEBUG
	public List<String> showIndex(){
		return this.indexes;
	}


	@POST
	@Path("/uploadData")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadData(@FormDataParam("file") InputStream uploadedInputStream,
	                           @FormDataParam("file") FormDataContentDisposition fileDetail)
			throws InvalidFileException {
		String location = "./" + fileDetail.getFileName();
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
			} catch (IOException e) { e.printStackTrace(); }
		}
		return Response.status(200).entity("File successfully uploaded to " + location).build();
	}


	@GET
	@Path("/getRows")
	public void getRows()  {
		// TODO
	}
	
}
