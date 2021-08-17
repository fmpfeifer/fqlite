package fqlite.util;

import fqlite.base.SqliteRow;

/**
 * Container class. It is used to return some result from 
 * carving back to the calling thread. 
 * 
 * @author pawlaszc
 *
 */
public class CarvingResult {

	public SqliteRow row;
	public int rcursor;
	public int offset; 
	
	public CarvingResult(int rcursor,int offset, SqliteRow result)
	{
		row = result;
		this.rcursor = rcursor;
		this.offset  = offset;
	}
}
