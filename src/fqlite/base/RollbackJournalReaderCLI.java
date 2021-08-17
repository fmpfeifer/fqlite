package fqlite.base;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
public class RollbackJournalReaderCLI extends RollbackJournalReaderBase {

	/**
	 * Constructor.
	 * 
	 * @param path    full qualified file name to the RollbackJournal archive
	 * @param job reference to the Job class
	 */
	public RollbackJournalReaderCLI(String path, JobCLI job) {
	    super(path, job);
	}

	/**
	 *  This method can be used to write the result to a file or
	 *  to update tables in the user interface (in gui-mode). 
	 */
	public void output()
	{
 	    JobCLI job = (JobCLI) this.job;
		Path dbfilename = Paths.get(path);
		String name = dbfilename.getFileName().toString();

		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter df;
		df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
		String date = df.format(now);

		String filename = "results" + name + date + ".csv";
		
		String[] lines = output.stream().map(SqliteRow::toString).toArray(String[]::new);
		job.writeResultsToFile(filename, lines);	
	}
}
