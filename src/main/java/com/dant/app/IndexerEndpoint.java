package com.dant.app;

import com.dant.entity.Table;
import com.dant.exception.InvalidIndexException;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IndexerEndpoint {
	private Table table;


	@POST
	@Path("/createTable")
	public boolean createTable(List<String> cols){
		System.out.println("Received columns " + cols);
		this.table = new Table(cols);
		return true;
	}


	@GET
	@Path("/showTable") // DEBUG
	public Table showTable(){
		return this.table;
	}


	@POST
	@Path("/addIndex")
	public boolean addIndex(List<String> indexesToAdd) throws InvalidIndexException {
		System.out.println("Received indexes to add: " + indexesToAdd);
		if(!this.table.getCols().containsAll(indexesToAdd)){
			List<String> invalidIndexes = new ArrayList<>();
			for(String s: indexesToAdd){
				if(!this.table.getCols().contains(s)){
					invalidIndexes.add(s);
				}
			}
			throw new InvalidIndexException(invalidIndexes.toString());
		}
		this.table.setIndexes(indexesToAdd);
		return true;
	}


	@GET
	@Path("/showIndex") // DEBUG
	public List<String> showIndex(){
		return this.table.getIndexes();
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
