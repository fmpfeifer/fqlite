package fqlite.base;

import java.util.stream.Collectors;

import fqlite.util.Logger;


public class JobCLI extends Job {
    
    public JobCLI() {
        super();
    }
    
    protected void linesReady() {
        String[] lines = getRows().stream().map(SqliteRow::toString).collect(Collectors.toList()).toArray(new String[0]);
        // String[] lines = ll.toArray(new String[0]);
        writeResultsToFile(null, lines);

        if (readRollbackJournal) {
            /* the readWAL option is enabled -> check the WAL-file too */
            Logger.out.info(" RollbackJournal-File " + this.rollbackjournalpath);
            rol = new RollbackJournalReaderCLI(rollbackjournalpath, this);
            rol.ps = this.ps;
            /* start parsing Rollbackjournal-file */
            rol.parse();
            rol.output();
        }
        else if (readWAL) {
            /* the readWAL option is enabled -> check the WAL-file too */
            Logger.out.info(" WAL-File " + walpath);
            WALReaderCLI wal = new WALReaderCLI(walpath, this);
            /* start parsing WAL-file */
            wal.parse();
            wal.output();
        }
    }
	
}
