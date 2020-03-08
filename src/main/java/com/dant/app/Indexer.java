package com.dant.app;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Indexer {
	List<String> cols;

	@GET
	@Path("/createTable")
	public List<String> createTable(@QueryParam("cols") List<String> cols){
		this.cols = cols;
		return cols;
	}

	
}
