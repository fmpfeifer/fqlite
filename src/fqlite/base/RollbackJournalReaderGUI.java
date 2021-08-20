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
 * The class analyses a Rollback Journal file and writes the found records into a file.
 * 
 * From the SLite documentation:
 * 
 * "The rollback journal is usually created when a transaction is first started and 
 *  is usually deleted when a transaction commits or rolls back. The rollback journal file 
 *  is essential for implementing the atomic commit and rollback capabilities of SQLite."
 * 
 * 
 * 
 * @author pawlaszc
 *
 */
public class RollbackJournalReaderGUI extends RollbackJournalReaderBase {
	
	HexView hexview = null;

	/**
	 * Constructor.
	 * 
	 * @param path    full qualified file name to the RollbackJournal archive
	 * @param job reference to the Job class
	 */
	public RollbackJournalReaderGUI(String path, JobGUI job) {
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
			
			info("Number of records recovered: " + output.size());

			String[] lines = output.stream().map(SqliteRow::toString).toArray(String[]::new);
			Arrays.sort(lines);

			TreePath path  = null;
			for (String line : lines) {
				String[] data = line.split(";");

				path = job.guiroltab.get(data[0]);
				job.gui.update_table(path, data, false);
				
			}
			
			/* remove empty tables and index-tables from the treeview */
			DefaultTreeModel model = (DefaultTreeModel) (GUI.tree.getModel());
			  
			//while(iter.hasNext())
			for (Entry<String, TreePath> entry : job.guiroltab.entrySet())
			{
				DefaultMutableTreeNode node = (DefaultMutableTreeNode)entry.getValue().getLastPathComponent();
				NodeObject no = (NodeObject)node.getUserObject();	
				
				/* check, if Rollback Journal-table has row entries, if not -> skip it */
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

			SwingWorker<Boolean, Void> backgroundProcess = new HexViewCreator(job,path,file,this.path,2);

			backgroundProcess.execute();
		}

	}

}
