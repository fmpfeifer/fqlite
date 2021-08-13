package fqlite.ui;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.FlavorEvent;
import java.awt.datatransfer.FlavorListener;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.event.ActionEvent;
import java.io.IOException;
import javax.swing.AbstractAction;
import javax.swing.JTextArea;

public class PasteActionTA extends AbstractAction {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JTextArea ta;

    public PasteActionTA(JTextArea ta) {
        this.ta = ta;
        putValue(NAME, "Paste");
        final Clipboard cb = Toolkit.getDefaultToolkit().getSystemClipboard();
        cb.addFlavorListener(new FlavorListener() {
			
            @Override
            public void flavorsChanged(FlavorEvent e) {
                setEnabled(cb.isDataFlavorAvailable(CellTransferable.CELL_DATA_FLAVOR));
            }
        });
        setEnabled(cb.isDataFlavorAvailable(CellTransferable.CELL_DATA_FLAVOR));
    }
    
    @Override
    public void actionPerformed(ActionEvent e) {
      
        Clipboard cb = Toolkit.getDefaultToolkit().getSystemClipboard();
        if (cb.isDataFlavorAvailable(CellTransferable.CELL_DATA_FLAVOR)) {
            try {
                Object value = cb.getData(CellTransferable.CELL_DATA_FLAVOR);
                if (value instanceof String)
                	ta.append((String)value);
            } catch (UnsupportedFlavorException | IOException ex) {
                ex.printStackTrace();
            }
        }
    }

}    