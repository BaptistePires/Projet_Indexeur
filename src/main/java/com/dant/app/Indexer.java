package com.dant.app;

import com.dant.exception.InvalidIndexException;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Indexer {
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
	@Path("/loadData")
	public void loadData()  {
		// TODO
	}


	@GET
	@Path("/getRows")
	public void getRows()  {
		// TODO
	}
	
}
