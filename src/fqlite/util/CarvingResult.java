package fqlite.util;

import fqlite.base.SqliteInternalRow;

/**
 * Container class. It is used to return some result from 
 * carving back to the calling thread. 
 * 
 * @author pawlaszc
 *
 */
public class CarvingResult {

	public SqliteInternalRow row;
	public int rcursor;
	public int offset; 
	
	public CarvingResult(int rcursor,int offset, SqliteInternalRow result)
	{
		row = result;
		this.rcursor = rcursor;
		this.offset  = offset;
	}
}
