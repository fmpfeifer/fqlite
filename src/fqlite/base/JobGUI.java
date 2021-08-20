package fqlite.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.swing.JOptionPane;
import javax.swing.SwingWorker;
import javax.swing.tree.TreePath;

import fqlite.base.WALReaderBase.WALFrame;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.ui.DBPropertyPanel;
import fqlite.ui.HexView;
import fqlite.ui.RollbackPropertyPanel;
import fqlite.ui.WALPropertyPanel;
import fqlite.util.Logger;


public class JobGUI extends Job {

	/* hex editor object reference */
	HexView hexview;

	/* since version 1.2 - support for write ahead logs WAL */
	Hashtable<String, TreePath> guiwaltab = new Hashtable<String, TreePath>();

	Hashtable<String, TreePath> guiroltab = new Hashtable<String, TreePath>();
	
    /* the next fields hold informations for the GUI */
	GUI gui = null;
	Hashtable<String, TreePath> guitab = new Hashtable<String, TreePath>();
	
	/* property panel for the user interface - only in gui-mode */
	DBPropertyPanel panel = null;
	WALPropertyPanel walpanel = null;
	RollbackPropertyPanel rolpanel = null;
	
	public void setPropertyPanel(DBPropertyPanel p)
	{
		this.panel = p;
	}
	
	public void setWALPropertyPanel(WALPropertyPanel p)
	{
		this.walpanel = p;
	}
	

	public void setRollbackPropertyPanel(RollbackPropertyPanel p)
	{
		this.rolpanel = p;
	}
	
	
	public String[][] getHeaderProperties()
	{
		String [][] prop = {{"0","The header string",headerstring},
				{"16","The database page size in bytes",String.valueOf(ps)},
				{"18","File format write version",String.valueOf(ffwversion)},
				{"19","File format read version",String.valueOf(ffrversion)},
				{"20","Unused reserved space at the end of each page ",String.valueOf(reservedspace)},
				{"21","Maximum embedded payload fraction. Must be 64.",String.valueOf(maxpayloadfrac)},
				{"22","Minimum embedded payload fraction. Must be 32.",String.valueOf(minpayloadfrac)},
				{"23","Leaf payload fraction. Must be 32.",String.valueOf(leafpayloadfrac)},
				{"24","File change counter.",String.valueOf(filechangecounter)},
				{"28","Size of the database file in pages. ",String.valueOf(sizeinpages)},
				{"32","Page number of the first freelist trunk page.",String.valueOf(fphead)},
				{"36","Total number of freelist pages.",String.valueOf(fpnumber)},
				{"40","The schema cookie.",String.valueOf(schemacookie)},
				{"44","The schema format number. Supported schema formats are 1, 2, 3, and 4.",String.valueOf(schemaformatnumber)},
				{"48","Default page cache size.",String.valueOf(defaultpagecachesize)},
				{"52","The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.",String.valueOf(avacc)+" (" + (avacc > 0 ? true : false) + ")"},
				{"56","The database text encoding.",String.valueOf(db_encoding.displayName())},
				{"60","The \"user version\"",String.valueOf(userversion)},
				{"64","True (non-zero) for incremental-vacuum mode. False (zero) otherwise.",String.valueOf(vacuummode)+" (" + (vacuummode > 0 ? true : false) + ")"},
				{"92","The version-valid-for number.",String.valueOf(versionvalidfornumber)},
				{"96","SQLITE_VERSION_NUMBER",String.valueOf(sqliteversion)}};
		return prop;
	}
	
	public String[][] getWALHeaderProperties()
	{
		String [][] prop = {{"0","HeaderString",wal.headerstring},
				{"4","File format version",String.valueOf(wal.ffversion)},
				{"8","Database page size",String.valueOf(wal.ps)},
				{"12","Checkpoint sequence number",String.valueOf(wal.csn)},
				{"16","Salt-1",String.valueOf(wal.hsalt1)},
				{"20","Salt-2",String.valueOf(wal.hsalt2)},
				{"24","Checksum1",String.valueOf(wal.hchecksum1)},
				{"28","Checksum2",String.valueOf(wal.hchecksum2)}};
		
		return prop;
	}
	
	public String[][] getRollbackHeaderProperties()
	{
		String [][] prop = {{"0","HeaderString",RollbackJournalReaderBase.MAGIC_HEADER_STRING},
				{"8","number of pages",String.valueOf(rol.pagecount)},
				{"12","nounce for checksum",String.valueOf(rol.nounce)},
				{"16","pages",String.valueOf(rol.pages)},
				{"20","sector size ",String.valueOf(rol.sectorsize)},
				{"24","journal page size",String.valueOf(rol.journalpagesize)}};
		
		return prop;
	}
	
	public String[][] getCheckpointProperties()
	{
		ArrayList<String []> prop = new ArrayList<String []>();
		
		Set<Long> data = wal.checkpoints.descendingKeySet();
		
		Iterator<Long> it = data.iterator();
		
		while (it.hasNext())
		{
			Long salt1 = it.next();
			
			LinkedList<WALFrame> list = wal.checkpoints.get(salt1);
			
			Iterator<WALFrame> frames = list.iterator();
			
			while (frames.hasNext())
			{
				WALFrame current = frames.next();
				
			    String[] line = new String[5];
			    line[0] = String.valueOf(current.salt1);
			    line[1] = String.valueOf(current.salt2);
			    line[2] = String.valueOf(current.framenumber);
			    line[3] = String.valueOf(current.pagenumber);
			    line[4] = String.valueOf(current.committed);
			    
			    prop.add(line);
			    
			}
		}
		
		String[][] result = new String[prop.size()][5];
		prop.toArray(result);
		
		return result;
	}
	
	public LinkedHashMap<String,String[][]> getTableColumnTypes() 
	{
		
		LinkedHashMap<String,String[][]> ht = new LinkedHashMap<String,String[][]>();
		
		
		for (TableDescriptor td : headers.values())
		{
		    String[] names = (String[])td.columnnames.toArray(new String[0]);
		    String[] types = (String[])td.serialtypes.toArray(new String[0]);
		    String[] sqltypes = (String[])td.sqltypes.toArray(new String[0]);
		    String[] tableconstraints = null;
		    String[] constraints = null;
		    
			/* check, if there exists global constraints to the table */
		    if (null != td.tableconstraints)
	    	{	
	    		tableconstraints = (String[])td.tableconstraints.toArray(new String[0]);	    		
	    	}
		    /* check, if there are constraints on one of the columns */
		    if (null != td.constraints)
		    {
		    	constraints = (String[])td.constraints.toArray(new String[0]);    			    	
		    }
		    
		    String[][] row = null;
		    if(null != tableconstraints && null != constraints)
		    {
			    row = new String[][]{names,types,sqltypes,constraints,tableconstraints};
		    		    	
		    }
		    else if (null != tableconstraints)
		    {
		       	row = new String[][]{names,types,sqltypes,tableconstraints};
				 
		    }
		    else if (null != constraints)
		    {
		       	row = new String[][]{names,types,sqltypes,constraints};
				    	
		    }
		    else
		    {
		    	row = new String[][]{names,types,sqltypes};
		    }
		    
			ht.put(td.tblname,row);
		}
		
		for (IndexDescriptor id : indices.values()) {
			String[] names = (String[])id.columnnames.toArray(new String[0]);
			String[] types = (String[])id.columntypes.toArray(new String[0]);
		    
		    String[][] row = null;
	    	row = new String[][]{names,types};

			ht.put("idx:" + id.idxname,row);
		}	

		return ht;
	}
	

	
	public String[][] getSchemaProperties()
	{
		String [][] prop = new String[headers.size() + indices.size()][6];//{{"","",""},{"","",""}};
		int counter = 0;
		
		for (TableDescriptor td : headers.values())
		{
		    if (!td.tblname.startsWith("__"))
		    	prop[counter] = new String[]{"Table",td.tblname,String.valueOf(td.root),td.sql,String.valueOf(td.isVirtual()),String.valueOf(td.ROWID)};
			counter++;			
		}

		for (IndexDescriptor td : indices.values()) {

			prop[counter] = new String[]{"Index",td.idxname,String.valueOf(td.root),td.getSql(),"",""};
			counter++;			
		}

		return prop;
	}

	
	public void updateRollbackPanel()
	{
		rolpanel.initHeaderTable(this.getRollbackHeaderProperties());	
	}
	
	public void updatePropertyPanel()
	{
		panel.initHeaderTable(getHeaderProperties());
		panel.initSchemaTable(getSchemaProperties());
		panel.initColumnTypesTable(getTableColumnTypes());
	}

	public void updateWALPanel()
	{
		walpanel.initHeaderTable(getWALHeaderProperties());
		walpanel.initCheckpointTable(getCheckpointProperties());
	}
	
	protected void tableDescriptorReady(TableDescriptor td) {
	    TreePath path = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(), td.primarykeycolumns, false, false,0);
        guitab.put(td.tblname, path);

        if (readWAL) {
            
            List<String> cnames = td.columnnames;
            cnames.add(0,"commit");
            cnames.add(1,"dbpage");
            cnames.add(2,"walframe");
            cnames.add(3,"salt1");
            cnames.add(4,"salt2");
            
            List<String> ctypes = td.serialtypes;
            ctypes.add(0,"INT");
            ctypes.add(1,"INT");
            ctypes.add(2,"INT");
            ctypes.add(3,"INT");
            ctypes.add(4,"INT");
            
            
            TreePath walpath = gui.add_table(this, td.tblname, cnames, ctypes, td.primarykeycolumns, true, false,0);
            guiwaltab.put(td.tblname, walpath);
            setWALPath(walpath.toString());

        }

        else if (readRollbackJournal) {
            TreePath rjpath = gui.add_table(this, td.tblname, td.columnnames, td.getColumntypes(),td.primarykeycolumns, false, true,0);
            guiroltab.put(td.tblname, rjpath);
            setRollbackJournalPath(rjpath.toString());
        }

        if (null != path) {
            Logger.out.info("Expend Path" + path);
            GUI.tree.expandPath(path);
        }
    }
    
    protected void indexDescriptorReady(IndexDescriptor id) {
        TreePath path = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, null, false, false,1);
        Logger.out.info("id.idxname " + id.idxname);
        guitab.put(id.idxname, path);

        if (readWAL) {
            List<String> cnames = id.columnnames;
            cnames.add(0,"commit");
            cnames.add(1,"dbpage");
            cnames.add(2,"walframe");
            cnames.add(3,"salt1");
            cnames.add(4,"salt2");
            
            List<String> ctypes = id.columntypes;
            ctypes.add(0,"INT");
            ctypes.add(1,"INT");
            ctypes.add(2,"INT");
            ctypes.add(3,"INT");
            ctypes.add(4,"INT");
            
            TreePath walpath = gui.add_table(this, id.idxname, cnames, ctypes, null, true, false,1);
            guiwaltab.put(id.idxname, walpath);
            setWALPath(walpath.toString());

        }

        else if (readRollbackJournal) {
            TreePath rjpath = gui.add_table(this, id.idxname, id.columnnames, id.columntypes, null, false, true,1);
            guiroltab.put(id.idxname, rjpath);
            setRollbackJournalPath(rjpath.toString());
        }

        if (null != path) {
            GUI.tree.expandPath(path);
        }
    }
    
    protected void unassignedTableCreated(TableDescriptor tdefault) {
        TreePath path = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
                tdefault.getColumntypes(), null, false, false,0);
        guitab.put(tdefault.tblname, path);

        if (readWAL) {
            TreePath walpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
                    tdefault.getColumntypes(), null, true, false,0);
            guiwaltab.put(tdefault.tblname, walpath);
            setWALPath(walpath.toString());
        }

        else if (readRollbackJournal) {
            TreePath rjpath = gui.add_table(this, tdefault.tblname, tdefault.columnnames,
                    tdefault.getColumntypes(),null, false, true,0);
            guiroltab.put(tdefault.tblname, rjpath);
            setRollbackJournalPath(rjpath.toString());
        }

        if (null != path) {
            GUI.tree.expandPath(path);
        }
    }
    
    protected void linesReady() throws IOException {
        info("Number of records recovered: " + getRows().size());

        //String[] lines = ll.toArray(new String[0]);
        String[] lines = getRows().stream().map(SqliteRow::toString).toArray(String[]::new);
        Arrays.sort(lines);

        TreePath path = null;
        for (String line : lines) {
            String[] data = line.split(";");

            path = guitab.get(data[0]);
            gui.update_table(path, data, false);
        }

        SwingWorker<Boolean, Void> backgroundProcess = new HexViewCreator(this, path, file, this.path, 0);

        
        backgroundProcess.execute();

        if (GUI.doesWALFileExist(this.path) > 0) {

            String walpath = this.path + "-wal";
            wal = new WALReaderGUI(walpath, this);
            wal.parse();
            wal.output();

        }

        if (GUI.doesRollbackJournalExist(this.path) > 0) {

            String rjpath = this.path + "-journal";
            rol = new RollbackJournalReaderGUI(rjpath, this);
            rol.ps = this.ps;
            rol.parse();
            rol.output();
        }
    }
	

	protected void setGUI(GUI gui) {
		this.gui = gui;
	}

	public void info(String message) {
		if (gui != null)
			gui.doLog(message);
		else
			super.info(message);
	}

	public void err(String message) {
		if (gui != null)
			JOptionPane.showMessageDialog(gui, message);
		else
			super.err("ERROR: " + message);
	}
	
	@Override
	protected void closeResources() {
	    // Do not close files when in GUI mode, as it may be necessary to read data from
	    // the files
	}
	
	/**
	 *	 
	 */
	public JobGUI() {
		super();
	}


}