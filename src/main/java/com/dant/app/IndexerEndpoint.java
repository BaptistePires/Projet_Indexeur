package com.dant.app;

import com.dant.entity.Account;
import com.dant.entity.Column;
import com.dant.entity.Table;
import com.dant.exception.InvalidIndexException;
import com.dant.exception.UnsupportedTypeException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/indexer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class IndexerEndpoint {
	private Table table;


	@POST
	@Path("/createTable")
	public boolean createTable(String body) throws UnsupportedTypeException {
		JsonObject columns = new JsonParser().parse(body).getAsJsonObject();
		table = new Table();
		for(Map.Entry<String, JsonElement>  col: columns.entrySet()){
			table.addColumn(new Column(col.getKey(), col.getValue().getAsString()));
		}
		return true;
	}


	@GET
	@Path("/showTable") // DEBUG
	public Table showTable(){
		return table;
//		return Response.status(200).entity(table).build();
	}


	@POST
	@Path("/addIndex")
	public boolean addIndex(List<String> indexesToAdd) throws InvalidIndexException {
//		System.out.println("Received indexes to add: " + indexesToAdd);
//		if(!this.table.getCols().containsAll(indexesToAdd)){
//			List<String> invalidIndexes = new ArrayList<>();
//			for(String s: indexesToAdd){
//				if(!this.table.getCols().contains(s)){
//					invalidIndexes.add(s);
//				}
//			}
//			throw new InvalidIndexException(invalidIndexes.toString());
//		}
//		this.table.setIndexes(indexesToAdd);
		return true;
	}


//	@GET
//	@Path("/showIndex") // DEBUG
//	public List<String> showIndex(){
//		return this.table.getIndexes();
//	}


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
