package fqlite.ui;

import java.awt.Point;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JPopupMenu;
import javax.swing.JTable;
import javax.swing.JTextArea;

public class PopupFactory {

	
	public static void createPopup(JTextArea jt)
	{
		/* create popup menu */
		final JPopupMenu pm = new JPopupMenu();
		pm.add(new CopyActionTA(jt));
		pm.add(new PasteActionTA(jt));
		
		jt.setComponentPopupMenu(pm);
		jt.addMouseListener(new MouseAdapter() {	
			
			@Override
			public void mouseClicked(MouseEvent e) {
					
				if (e.isPopupTrigger()) {			
					doPopup(e);
				}
			}
				
			public void doPopup(MouseEvent e) {
					pm.show(e.getComponent(), e.getX(), e.getY());
			}
		});
	}
	
	public static void createPopup(JTable jt)
	{
		/* create popup menu */
		final JPopupMenu pm = new JPopupMenu();
		pm.add(new CopyAction(jt));
		pm.add(new PasteAction(jt));
		jt.setComponentPopupMenu(pm);
		jt.addMouseListener(new MouseAdapter() {	
			
			@Override
			public void mouseClicked(MouseEvent e) {
					
				if (e.isPopupTrigger()) {
					highlightRow(e);
					doPopup(e);
				}
			}
				
			public void doPopup(MouseEvent e) {
					pm.show(e.getComponent(), e.getX(), e.getY());
			}

			public void highlightRow(MouseEvent e) {
					JTable table = (JTable) e.getSource();
					Point point = e.getPoint();
					int row = table.rowAtPoint(point);
					int col = table.columnAtPoint(point);

					table.setRowSelectionInterval(row, row);
					table.setColumnSelectionInterval(col, col);
				}
			
		});
		
		
	}
}
