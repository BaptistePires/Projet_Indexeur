package com.dant.app;

import com.google.gson.Gson;
import com.dant.exception.InvalidIndexException;
import com.google.gson.GsonBuilder;
import org.omg.CORBA.DynAnyPackage.Invalid;

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
	List<String> indexs;
//	private final Gson gson = new GsonBuilder().serializeNulls().create();


	@GET
	@Path("/createTable")
	public List<String> createTable(@QueryParam("cols") List<String> cols){
		this.cols = Arrays.asList(cols.get(0).split(","));
		return this.cols;
	}

	@GET
	@Path("/showTable") // DEBUG
	public List<String> showTable(){
		return this.cols;
	}

	@GET
	@Path("/addIndex")
	public List<String> addIndex(@QueryParam("indexsToAdd") List<String> indexsToAdd) throws InvalidIndexException {
		if(!this.cols.containsAll(indexsToAdd)){
			StringBuilder sb = new StringBuilder();
			for(String s: indexsToAdd){
				if(!this.cols.contains(s)){
					System.out.println(s);
					sb.append(s).append(" ");
				}
			}
			throw new InvalidIndexException(sb.toString());
		}
		this.indexs = indexsToAdd;
		return this.indexs;
	}

	@GET
	@Path("/showIndex") // DEBUG
	public String showIndex(){
		return this.indexs.toString();
	}


	@POST
	@Path("/loadData")
	public void loadData()  {
		// TO DO
	}

	@GET
	@Path("/getRows")
	public void getRows()  {
		// TO DO
	}
	
}
