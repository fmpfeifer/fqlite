package fqlite.descriptor;

import fqlite.base.Base;

/**
 * An abstract base class for possible database 
 * objects like index,table, trigger or view.
 * 
 * At the moment FQLite only supports tables and
 * indices.
 * 
 * @author pawlaszc
 *
 */
public abstract class AbstractDescriptor extends Base {

	public boolean ROWID = true;
	
	/**
	 * Return the name of the database object.
	 * @return the name of the database object.
	 */
	abstract public String getName();
	
	
}
