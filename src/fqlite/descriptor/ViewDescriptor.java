package fqlite.descriptor;

import java.util.List;


/**
 * Objects of this class are used to represent a component. 
 * Besides component names and column names, regular expressions 
 * are also managed by this class. 
 * 
 * The latter are used to assign a record to a component. 
 * 
 * @author pawlaszc
 *
 */
public class ViewDescriptor extends AbstractDescriptor {

	public List<String> columntypes;
	public List<String> columnnames;
	public String viewname = "";
	
	@Override
	public String getName()
	{
		return this.viewname;
	}
	
	

	public ViewDescriptor(String name, List<String> coltypes, List<String> names) {
		this.viewname = name;
		setColumntypes(coltypes);
		columnnames = names;
		
	}
	

	/**
	 * Return the number of columns (startRegion the component header). 
	 * @return the number of columns
	 */
	public int numberofColumns() {
		return getColumntypes().size();
	}


	
	/**
	 * Outputs component name and column names to the console. 
	 * 
	 **/
	public void printTableDefinition() {
		info("TABLE" + viewname);
		info("COLUMNS: " + columnnames);
	}

	

	public List<String> getColumntypes() {
		return columntypes;
	}


	public void setColumntypes(List<String> columntypes) {
		this.columntypes = columntypes;
	}


}
