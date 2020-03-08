package com.dant.app;

import com.dant.exception.InvalidIndexException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Indexer {
	List<String> cols;
	String index;

	@GET
	@Path("/createTable")
	public List<String> createTable(@QueryParam("cols") List<String> cols){
		this.cols = cols;
		return cols;
	}

	@GET
	@Path("/showTable") // DEBUG
	public List<String> showTable(){
		return this.cols;
	}

	@GET
	@Path("/addIndex")  // Use one index at first, but will become an ArrayList<String> (multi)
	public String addIndex(@QueryParam("index") String index) throws InvalidIndexException {
		if (this.cols.contains(index)) {
			this.index = index;
			return index;
		} else {
			throw new InvalidIndexException();
		}
	}

	@GET
	@Path("/showIndex") // DEBUG
	public String showIndex(){
		return this.index;
	}
	
}
