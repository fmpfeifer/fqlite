package fqlite.base;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;

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
public class WALReaderCLI extends WALReaderBase {
	

	/**
	 * Constructor.
	 * 
	 * @param path    full qualified file name to the WAL archive
	 * @param job reference to the Job class
	 */
	public WALReaderCLI(String path, JobCLI job) {
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
		
		String[] lines = output.stream().map(SqliteRow::toString).collect(Collectors.toList()).toArray(new String[0]);
		job.writeResultsToFile(filename,lines);
	}
}