package fqlite.base;

import java.io.IOException;

import fqlite.util.Logger;


public class JobCLI extends Job {
    
    public JobCLI() {
        super();
    }
    
    protected void linesReady() throws IOException {
        String[] lines = getRows().stream().map(SqliteInternalRow::toString).toArray(String[]::new);
        // String[] lines = ll.toArray(new String[0]);
        writeResultsToFile(null, lines);

        if (readRollbackJournal) {
            /* the readWAL option is enabled -> check the WAL-file too */
            Logger.out.info(" RollbackJournal-File ", this.rollbackjournalpath);
            rol = new RollbackJournalReaderCLI(rollbackjournalpath, this);
            rol.ps = this.ps;
            /* start parsing Rollbackjournal-file */
            rol.parse();
            rol.output();
        }
        else if (readWAL) {
            /* the readWAL option is enabled -> check the WAL-file too */
            Logger.out.info(" WAL-File ", walpath);
            WALReaderCLI wal = new WALReaderCLI(walpath, this);
            /* start parsing WAL-file */
            wal.parse();
            wal.output();
        }
    }
	
}
