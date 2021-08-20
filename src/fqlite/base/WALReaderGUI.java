package fqlite.base;

import java.util.Arrays;
import java.util.Map.Entry;

import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import fqlite.ui.DBTable;
import fqlite.ui.HexView;
import fqlite.ui.NodeObject;

/**
 * The class analyzes a WAL-file and writes the found records into a file.
 * 
 * From the SQLite documentation:
 * 
 * "The original content is preserved in the database file and the changes are appended into a separate WAL file. 
 *  A COMMIT occurs when a special record indicating a commit is appended to the WAL. Thus a COMMIT can happen 
 *  without ever writing to the original database, which allows readers to continue operating from the original 
 *  unaltered database while changes are simultaneously being committed into the WAL. Multiple transactions can be 
 *  appended to the end of a single WAL file."
 * 
 * @author pawlaszc
 *
 */
public class WALReaderGUI extends WALReaderBase {
	
	public HexView hexview = null;
	
	/**
	 * Constructor.
	 * 
	 * @param path    full qualified file name to the WAL archive
	 * @param job reference to the Job class
	 */
	public WALReaderGUI(String path, JobGUI job) {
	    super(path, job);
	}

	/**
	 *  This method can be used to write the result to a file or
	 *  to update tables in the user interface (in gui-mode). 
	 */
	public void output()
	{
	    JobGUI job = (JobGUI) this.job;
		if (job.gui != null) {
			
			
			info("Number of records recovered: " + output.size() + output.toString());

			String[] lines = output.stream().map(SqliteRow::toString).toArray(String[]::new);
			Arrays.sort(lines);

	
			TreePath path  = null;
			for (String line : lines) {
				String[] data = line.split(";");

				path = job.guiwaltab.get(data[0]);
				job.gui.update_table(path, data, true);
				
			}
			
			
			/* remove empty tables and index-tables from the treeview */
			DefaultTreeModel model = (DefaultTreeModel) (GUI.tree.getModel());
			  
			//while(iter.hasNext())
			for (Entry<String, TreePath> entry : job.guiroltab.entrySet())
			{
				DefaultMutableTreeNode node = (DefaultMutableTreeNode)entry.getValue().getLastPathComponent();
				NodeObject no = (NodeObject)node.getUserObject();	
				
				/* check, if WAL-table has row entries, if not -> skip it */
				DBTable t = no.table;
				
				 if (t.getModel().getRowCount() <= 1)
				 {
					 SwingUtilities.invokeLater(new Runnable() {
						    public void run() {
						    	TreeNode parent = node.getParent();
							    model.removeNodeFromParent(node);
							    model.nodeChanged(parent); 
							    model.reload(parent);
							    GUI.tree.updateUI();
						    }
					 });
					    
					    
	    		       
	    		 }
				
			}	
		
			/* create a hex viewer object for this particular table */

			SwingWorker<Boolean, Void> backgroundProcess = new HexViewCreator(job,path,file,this.path,1);

			backgroundProcess.execute();

		}		
	}
	
}
