package fqlite.base;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import fqlite.descriptor.AbstractDescriptor;
import fqlite.descriptor.IndexDescriptor;
import fqlite.descriptor.TableDescriptor;
import fqlite.parser.SQLiteSchemaParser;
import fqlite.util.Auxiliary;
import fqlite.util.ByteSeqSearcher;
import fqlite.util.Logger;
import fqlite.util.RandomAccessFileReader;


/*
---------------
Job.java
---------------
(C) Copyright 2020.

Original Author:  Dirk Pawlaszczyk
Contributor(s):   -;


Project Info:  http://www.hs-mittweida.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Dieses Programm ist Freie Software: Sie können es unter den Bedingungen
der GNU General Public License, wie von der Free Software Foundation,
Version 3 der Lizenz oder (nach Ihrer Wahl) jeder neueren
veröffentlichten Version, weiterverbreiten und/oder modifizieren.

Dieses Programm wird in der Hoffnung, dass es nützlich sein wird, aber
OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
Siehe die GNU General Public License für weitere Details.

Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.

*/
/**
 * Core application class. It is used to recover lost SQLite records from a
 * sqlite database file. As a carving utility, it is binary based and can recover
 * deleted entries from sqlite 3.x database files.
 * 
 * Note: For each database file exactly one Job object is created. This class has
 * a reference to the file on disk. 
 * 
 * From the sqlite web-page: "A database file might contain one or more pages
 * that are not in active use. Unused pages can come about, for example, when
 * information is deleted startRegion the database. Unused pages are stored on the
 * freelist and are reused when additional pages are required."
 * 
 * This class makes use of this behavior.
 * 
 * 
 * 
 *     __________    __    _ __     
 *    / ____/ __ \  / /   (_) /____ 
 *   / /_  / / / / / /   / / __/ _ \
 *  / __/ / /_/ / / /___/ / /_/  __/
 * /_/    \___\_\/_____/_/\__/\___/ 
 * 
 * 
 * 
 * @author Dirk Pawlaszczyk
 * @version 1.2
 */
public class Job extends Base {
	
	/* since version 1.2 - support for write ahead logs WAL */
	public boolean readWAL = false;
	String walpath = null;
	WALReaderBase wal = null;

	/* since version 1.2 - support for write Rollback Journal files */
	public boolean readRollbackJournal = false;
	String rollbackjournalpath = null;
	RollbackJournalReaderBase rol = null;
	
	
    /* some constants */
	final static String MAGIC_HEADER_STRING = "53514c69746520666f726d6174203300";
	final static String NO_AUTO_VACUUM = "00000000";
	final static String NO_MORE_ENTRIES = "00000000";
	final static String LEAF_PAGE = "0d";
	final static String INTERIOR_PAGE = "05";
	final static String ROW_ID = "00";
	
	
	/* path - used to locate a file in a file system */
	String path;
	
	/* A channel for reading, writing, and manipulating the database file. */
	public RandomAccessFileReader file;
	
	/* this field represent the database encoding */
	public Charset db_encoding = StandardCharsets.UTF_8;
	
	/* this is a multi-threaded program -> all data are saved to the list first*/
	private Queue<SqliteRow> ll = new ConcurrentLinkedQueue<>();

	private Map<String, List<SqliteRow>> tableRows = Collections.synchronizedMap(new LinkedHashMap<>());
	
	/* header fields */
	
	String headerstring;
	byte ffwversion;
	byte ffrversion;
	byte reservedspace;
	byte maxpayloadfrac;
	byte minpayloadfrac;
	byte leafpayloadfrac;
	long filechangecounter;
	long inheaderdbsize;
	long sizeinpages;
	long schemacookie;
	long schemaformatnumber;
	long defaultpagecachesize;
	long userversion;
	long vacuummode;
	long versionvalidfornumber;
	long avacc;
	
	/* Virtual Tables */
	public Map<String, TableDescriptor> virtualTables = new LinkedHashMap<String,TableDescriptor>();
	
	int scanned_entries = 0;
	
	Hashtable<String, String> tblSig;
	boolean is_default = false;
	public Map<String, TableDescriptor> headers = new LinkedHashMap<String, TableDescriptor>();
	public Map<String, IndexDescriptor> indices = new LinkedHashMap<String, IndexDescriptor>();
	public AtomicInteger runningTasks = new AtomicInteger();
	int tablematch = 0;
	int indexmatch = 0; 
	
	AtomicInteger numberofcells = new AtomicInteger();
	
	Set<Integer> allreadyvisit;
	
	/* this array holds a description of the associated component for each data page, if known */
	public AbstractDescriptor[] pages;
	
	/* each db-page has only one type */
	int[] pagetype;
	
	/* page size */
	public int ps = 0;
	public int numberofpages = 0;
	/* free page pragma fields in db header */
	int fpnumber = 0;
	int fphead = 0;
	String sqliteversion = "";
	boolean autovacuum = false;
	
	/* if a page has been scanned it is marked as checked */
	AtomicReferenceArray<Boolean> checked;
	
	public AtomicInteger hits = new AtomicInteger();
	
	/** Collect resources to close at end of processing */
	private Deque<Closeable> resourcesToClose = new LinkedList<>();

	public boolean recoverOnlyDeletedRecords = false;

	public SQLiteSchemaParser schemaParser = new SQLiteSchemaParser();

	  
	/******************************************************************************************************/
	
	private RandomAccessFileReader readWAL(String walpath) throws IOException {
		
		Path p = Paths.get(walpath);
		if (! Files.exists(p)) {
		    return null;
		}

		/* First - try to analyze the db-schema */
		/*
		 * we have to do this before we open the database because of the concurrent
		 * access in multi threading mode
		 */

		/* try to open the wal-file in read-only mode */
		RandomAccessFileReader file = new RandomAccessFileReader(p);
		resourcesToClose.addFirst(file);
		return file;
		
	}
	
	protected void tableDescriptorReady(TableDescriptor td) {
	}
	
	protected void indexDescriptorReady(IndexDescriptor id) {
	}
	
	protected void unassignedTableCreated(TableDescriptor td) {
	}
	
	protected void linesReady() throws IOException {
	}
			
	/**
	 * This is the main processing loop of the program.
	 * 
	 * @return -1 if error, 0 if success
	 * @throws InterruptedException the InterruptedException
	 * @throws ExecutionException the ExecutionException
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public int processDB() throws InterruptedException, ExecutionException, IOException {

		allreadyvisit = ConcurrentHashMap.newKeySet();
		List<TableDescriptor> recoveryTables = new LinkedList<>();

		try {

			Path p = Paths.get(path);

			/* First - try to analyze the db-schema */
			/*
			 * we have to do this before we open the database because of the concurrent
			 * access
			 */

			/* try to open the db-file in read-only mode */
			file = new RandomAccessFileReader(p);
			resourcesToClose.add(file);

			/* read header of the sqlite db - the first 100 bytes */
			ByteBuffer buffer = file.allocateAndReadBuffer(100);

			/* The first 100 bytes of the database file comprise the database file header. 
			 * The database file header is divided into fields as shown by the table below. 
			 * All multibyte fields in the database file header are stored with the must 
			 * significant byte first (big-endian).
			 * 
			 * 0	16	The header string: "SQLite format 3\000"
			 * 16	 2	The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
			 * 18    1	File format write version. 1 for legacy; 2 for WAL.
		     * 19  	 1	File format read version. 1 for legacy; 2 for WAL.
			 * 20    1	Bytes of unused "reserved" space at the end of each page. Usually 0.
			 * 21	 1	Maximum embedded payload fraction. Must be 64.
			 * 22 	 1	Minimum embedded payload fraction. Must be 32.
			 * 23 	 1	Leaf payload fraction. Must be 32.
			 * 24  	 4	File change counter.
			 * 28 	 4	Size of the database file in pages. The "in-header database size".
			 * 32  	 4	Page number of the first freelist trunk page.
			 * 36	 4	Total number of freelist pages.
			 * 40 	 4	The schema cookie.
			 * 44 	 4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
			 * 48	 4	Default page cache size.
			 * 52	 4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
			 * 56	 4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
			 * 60	 4	The "user version" as read and set by the user_version pragma.
			 * 64	 4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
			 * 68	24	Reserved for expansion. Must be zero.
			 * 92	 4	The version-valid-for number.
			 * 96	 4	SQLITE_VERSION_NUMBER
			 */
			
			
			/********************************************************************/

			byte header[] = new byte[16];
			buffer.get(header);
			headerstring = Auxiliary.bytesToHex(header);
			char charArray[] = new char[16];
			
			int cn = 0;
			for (byte b: header)
			{
				charArray[cn] = (char)b;
				cn++;
			}
			String txt = new String(charArray);
			
			headerstring = txt + " (" + "0x" + headerstring + ")";
			
			if (Auxiliary.bytesToHex(header).equals(MAGIC_HEADER_STRING)) // we currently
			{
				// support
				// sqlite 3 data
				// bases
				info("header is okay. seems to be an sqlite database file.");
		    }
			else {
				info("sorry. doesn't seem to be an sqlite file. Wrong header.");
				err("Doesn't seem to be an valid sqlite file. Wrong header");
				return -1;
			}

			
			buffer.position(18);
			ffwversion = buffer.get();
			info("File format write version. 1 for legacy; 2 for WAL. " + ffwversion);

			buffer.position(19);
			ffrversion = buffer.get();
			info("File format read version. 1 for legacy; 2 for WAL. " + ffrversion);

			buffer.position(20);
		    reservedspace = buffer.get();
			info("Bytes of unused \"reserved\" space at the end of each page. Usually 0. " + reservedspace);

			maxpayloadfrac = buffer.get();
			info("Maximum embedded payload fraction. Must be 64." + maxpayloadfrac);

			minpayloadfrac = buffer.get();
			info("Minimum embedded payload fraction. Must be 32." + maxpayloadfrac);

			leafpayloadfrac = buffer.get();
			info("Leaf payload fraction. Must be 32.  " + leafpayloadfrac);

			buffer.position(16);
			inheaderdbsize = Integer.toUnsignedLong(buffer.getInt());
			if (inheaderdbsize == 1)
				inheaderdbsize = 65536;
			
			buffer.position(24);
			filechangecounter = Integer.toUnsignedLong(buffer.getInt());
			info("File change counter " + filechangecounter);

			buffer.position(28);
			sizeinpages = Integer.toUnsignedLong(buffer.getInt());
			info("Size of the database file in pages " + sizeinpages);

			buffer.position(40);
			schemacookie = Integer.toUnsignedLong(buffer.getInt());
			info("The schema cookie. (offset 40) " + schemacookie);

		    schemaformatnumber = Integer.toUnsignedLong(buffer.getInt());
			info("The schema format number. (offset 44) " + schemaformatnumber);
 
			defaultpagecachesize = Integer.toUnsignedLong(buffer.getInt());
			info("Default page cache size. (offset 48) " + defaultpagecachesize);
 
			buffer.position(60);
			
			userversion = Integer.toUnsignedLong(buffer.getInt());
			info("User version (offset 60) " + userversion);
 
			 
			vacuummode = Integer.toUnsignedLong(buffer.getInt());
			info("Incremential vacuum-mode (offset 64) " + vacuummode);
 
			buffer.position(92);

			versionvalidfornumber = Integer.toUnsignedLong(buffer.getInt());
			info("The version-valid-for number.  " + versionvalidfornumber);
 
			
					
			/********************************************************************/

			is_default = true;
			tblSig = new Hashtable<String, String>();
			tblSig.put("", "default");
			info("found unkown sqlite-database.");

			/********************************************************************/
			
			buffer.position(52);
			avacc = Integer.toUnsignedLong(buffer.getInt());
			if (avacc == 0) {
				info("Seems to be no AutoVacuum db. Nice :-).");
				autovacuum = true;
			} else
				autovacuum = false;

			/********************************************************************/
			// Determine database text encoding.
			// A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of
			// 3 means UTF-16be.
			byte[] encoding = new byte[4];
			buffer.position(56);
			buffer.get(encoding);
			int codepage = ByteBuffer.wrap(encoding).getInt();

			switch (codepage) {
			case 0:
			case 1:
				db_encoding = StandardCharsets.UTF_8;
				info("Database encoding: " + "UTF_8");
				break;
			case 3:
				db_encoding = StandardCharsets.UTF_16BE;
				info("Database encoding: " + "UTF_16BE");
				break;

			case 2:
				db_encoding = StandardCharsets.UTF_16LE;
				info("Database encoding: " + "UTF_16LE");
				break;

			}

			/*******************************************************************/

			/* 2 Byte big endian value */
			byte pagesize[] = new byte[2];
			/* at offset 16 */
			buffer.position(16);
			buffer.get(pagesize);

			ByteBuffer psize = ByteBuffer.wrap(pagesize);
			/*
			 * Must be a power of two between 512 and 32768 inclusive, or the value 1
			 * representing a page size of 65536.
			 */
			ps = Auxiliary.TwoByteBuffertoInt(psize);

			/*
			 * Beginning with SQLite version 3.7.1 (2010-08-23), a page size of 65536 bytes
			 * is supported.
			 */
			if (ps == 0 || ps == 1)
				ps = 65536;

			info("page size " + ps + " Bytes ");

			/*******************************************************************/

			/*
			 * get file size: Attention! We use the real file size information startRegion
			 * the file object not the header information!
			 */
			long totalbytes = file.size();

			/*
			 * dividing the number of bytes by pagesize we can compute the number of pages.
			 */
			numberofpages = (int) (totalbytes / ps);

			info("Number of pages:" + numberofpages);

			/*******************************************************************/

			/* determine the SQL-version of db on offset 96 */

			byte version[] = new byte[4];
			buffer.position(96);
			buffer.get(version);

			Integer v = ByteBuffer.wrap(version).getInt();
			sqliteversion = "" + v;

			/*******************************************************************/

			/* initialize some data structures */
			pages = new AbstractDescriptor[numberofpages + 1];
			pagetype = new int[numberofpages];
			checked = new AtomicReferenceArray<Boolean>(new Boolean[numberofpages]);

			/*******************************************************************/

			// byte[] pattern = null;
			byte[] tpattern = null;
			byte[] ipattern = null;
			int goback = 0;

			/* there are 3 possible encodings */
			if (db_encoding == StandardCharsets.UTF_8) {
				// pattern = new byte[]{7, 23};
				/* we are looking for the word 'table' */
				tpattern = new byte[] { 116, 97, 98, 108, 101 };
				/* we are looking for the word 'index' */
				ipattern = new byte[] { 105, 110, 100, 101, 120 };
				goback = 11;

			} else if (db_encoding == StandardCharsets.UTF_16LE) {
				/* we are looking for the word 'table' coding with UTF16LE */
				tpattern = new byte[] { 116, 00, 97, 00, 98, 00, 108, 00, 101 };
				/* we are looking for the word 'index' */
				ipattern = new byte[] { 105, 00, 110, 00, 100, 00, 101, 00, 120 };

				goback = 15;

			} else if (db_encoding == StandardCharsets.UTF_16BE) {
				/* we are looking for the word 'table' coding with UTF16BE */
				tpattern = new byte[] { 00, 116, 00, 97, 00, 98, 00, 108, 00, 101 };
				/* we are looking for the word 'index' */
				ipattern = new byte[] { 00, 105, 00, 110, 00, 100, 00, 101, 00, 120 };
				goback = 16;

			}

			
	
			
			/* we looking for the key word <table> of the type column */
			/* the mark the start of an entry for the sqlmaster_table */
			ByteSeqSearcher bsearch = new ByteSeqSearcher(tpattern);
			
			boolean again = false;
			int round = 0;
			RandomAccessFileReader bb = file;
			
			/**
			 * Step into loop
			 * 
			 * 1. iteration:  Try to read table and index information from normal database
			 * 
			 * 2. iteration:  No information found but a WAL archive in place - try to get the
			 *                missing information in a second look from the WAL archive
			 * 
			 */
			do
			{
				round++;
				
				if (round == 2 && !readWAL)
					break;
				
				long index = bsearch.indexOf(bb, 0);
	
				/* search as long as we can find further table keywords */
				while (index != -1) {
					/* get Header */
					byte mheader[] = new byte[40];
					bb.position(index - goback);
					bb.get(mheader);
					String headerStr = Auxiliary.bytesToHex(mheader);
	
					/**
					 * case 1: seems to be a dropped table data set
					 *
					 **/
					if (headerStr.startsWith("17") || headerStr.startsWith("21")) {
	
						headerStr = "07" + headerStr; // put the header length byte in front
						// note: for a dropped component this information is lost!!!
	
						tablematch++;
						/* try to get table master schema */
						if (round==1)
							readSchema(bb,index,headerStr, false);
						else
							readSchema(bb,index,headerStr, true);
							
					} 
					/**
					 *  case 2: Normal (possible) intact table with intact header
					 *
					 **/
					else if (headerStr.startsWith("0617")) {
						
						tablematch++;
						
						Auxiliary c = new Auxiliary(this);
						headerStr = headerStr.substring(0, 14);
	
						// compute offset
						int starthere = (int)(index % ps);
						int pagenumber = (int)(index / ps);
	
						ByteBuffer bbb = null;
						if (round == 1)
							bbb = readPageWithNumber(pagenumber, ps);
						else
						{
							starthere = (int) ((index - 32) % (ps + 24));
						    bbb = bb.allocateAndReadBuffer(32 + 24 + index/ps, ps);
						} if (db_encoding == StandardCharsets.UTF_8)
							c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
						else
							c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);
	
					} else {
						// false-positive match for <component> since no column type 17 (UTF-8) resp. 21
						// (UTF-16) was found
					}
					/* search for more component entries */
					index = bsearch.indexOf(bb, index + 2);
				}
	
				
				/**
				 *  Last but not least: Start Index-Search.
				 */
				
				/* we looking for the key word <index> */
				/* to mark the start of an indices entry for the sqlmaster_table */
				ByteSeqSearcher bisearch = new ByteSeqSearcher(ipattern);
	
				long index2 = bisearch.indexOf(bb, 0);
	
				/* search as long as we can find further index keywords */
				while (index2 != -1) {
	
					/* get Header */
					byte mheader[] = new byte[40];
					bb.position(index2 - goback);
					bb.get(mheader);
					String headerStr = Auxiliary.bytesToHex(mheader);
	
					Logger.out.info(headerStr);
	
					/**
					 * case 3: seems to be a dropped index data set
					 *
					 **/					
					 if (headerStr.startsWith("17") || headerStr.startsWith("21")) {
	
						headerStr = "07" + headerStr; // put the header length byte in front
						// note: for a dropped indices this information is lost!!!
	
						indexmatch++;
						Auxiliary c = new Auxiliary(this);
						headerStr = headerStr.substring(0, 14);
	
						// compute offset
						int starthere = (int) (index2 % ps);
						int pagenumber = (int) (index2 / ps);
	
						ByteBuffer bbb = null;
						if (round == 1)
						{	
							bbb = readPageWithNumber(pagenumber, ps);
							Logger.out.info("starthere 1st round " + starthere);

						}
						else
						{
							
							long pagebegin = 0L;
							// first page ?
							if (index2 < (ps + 56))
							{
								// skip WAL header (32 Bytes) and frame header (24 Bytes)
								pagebegin = 56;
								starthere  = (int) (index2 - 56);
							}
							else
							{
							    pagebegin = ((index2 - 32) / (ps + 24)) * (ps + 24) - 24 + 32;
								starthere = (int) ((index2 - 32) % (ps + 24) + 24);
							}
							
							/* go to frame start */
						    bbb = bb.allocateAndReadBuffer(pagebegin, ps);
							
							Logger.out.info("index match " + index2);

							Logger.out.info("ReaderWAL is true -> page begin offset " + pagebegin );

							Logger.out.info("ReaderWAL is true -> offset in page " + starthere);
							
							Logger.out.info(" index - starthere - page begin " + (index2 - starthere - pagebegin));

						}
						
	
						if (db_encoding == StandardCharsets.UTF_8)
							c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
						else if (db_encoding == StandardCharsets.UTF_16LE)
							c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);
						else if (db_encoding == StandardCharsets.UTF_16BE)
							c.readMasterTableRecord(this, starthere - 18, bbb, headerStr);
					
					} 
				    /**
				     * case 4: seems to be a regular index data set
				     * 
				     **/
					 
					 else if ((headerStr.startsWith("0617"))) {
						Auxiliary c = new Auxiliary(this);
						headerStr = headerStr.substring(0, 14);
	
						// compute offset
						int starthere = (int) (index2 % ps);
						int pagenumber = (int) (index2 / ps);
	
						ByteBuffer bbb = null;
						if (round == 1)
						{	
							bbb = readPageWithNumber(pagenumber, ps);
						}
						else
						{
							
							long pagebegin = 0;
							// first page ?
							if (index2 < (ps + 56))
							{
								// skip WAL header (32 Bytes) and frame header (24 Bytes)
								pagebegin = 56;
								starthere  = (int) (index2 - 56);
							}
							else
							{
							    pagebegin = ((index2 - 32) / (ps + 24)) * (ps + 24) - 24 + 32;
								starthere = (int) ((index2 - 32) % (ps + 24) + 24);
							}
							
							/* go to frame start */
							bbb = bb.allocateAndReadBuffer(pagebegin, ps);
							
							Logger.out.info("index match " + index2);

							Logger.out.info("ReaderWAL is true -> page begin offset " + pagebegin );

							Logger.out.info("ReaderWAL is true -> offset in page " + starthere);
							
							Logger.out.info(" index - starthere - page begin " + (index2 - starthere - pagebegin));
						}
						
						if (db_encoding == StandardCharsets.UTF_8)
							c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
						else
							c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);
	
					} else {
						// false-positive match for <component> since no column type 17 (UTF-8) resp. 21
						// (UTF-16) was found
					}
					/* search for more indices entries */
					index2 = bisearch.indexOf(bb, index2 + 2);
	
				}
			
				
				/* Is there a table schema definition inside the main database file ???? */
				Logger.out.info("headers:::: " + headers.size());
				
				if(headers.size()==0 && this.readWAL ==true)
				{
					// second try - maybe we could find a schema definition inside of the WAL-archive file instead?!
					
				    info("Could not find a schema definition inside the main db-file. Try to find something inside the WAL archive");
				
				    bb = readWAL(this.path+"-wal");
				    
				    if (null != bb)
				    	again = true;
					
				}
			
			}
			while(again && round < 2 && readWAL && !readRollbackJournal);

			/*******************************************************************/

			/*
			 * Since we now have all component descriptors - let us update the 
			 * indices descriptors
			 */

			
			for (IndexDescriptor id : indices.values()) {
				String tbn = id.tablename;
				
				if (null != tbn) {
				    TableDescriptor td = headers.get(tbn);

				    if (null != td) {

						/* component for index could be found */
						List<String> idname = id.columnnames;
						List<String> tdnames = td.columnnames;

						for (int z = 0; z < idname.size(); z++) {

							if (tdnames.contains(idname.get(z))) {
								/* we need to find the missing column types */
								try
								{
									int match = tdnames.indexOf(idname.get(z));
									String type = td.getColumntypes().get(match);
									id.columntypes.add(type);
									Logger.out.info("ADDING IDX TYPE::: " + type + " FOR TABLE " + td.tblname + " INDEx "
											+ id.idxname);
								}
								catch(Exception err)
								{
									
									id.columntypes.add("");
								}
								

							}

						}
						/**
						 * The index contains data from the columns that you specify in the index and
						 * the corresponding rowid value. This helps SQLite quickly locate the row based
						 * on the values of the indexed columns.
						 */

						id.columnnames.add("rowid");
						id.columntypes.add("INT");

						/* HeadPattern !!! */
						Auxiliary.addHeadPattern2Idx(id);

						exploreBTree(id.getRootOffset(), id);

						//break;
					}

				}
			}
			
			/*
			 * Now, since we have all component definitions, we can update the UI and start
			 * exploring the b-tree for each component
			 */
			for (TableDescriptor td : headers.values()) {

			    td.printTableDefinition();

				int r = td.getRootOffset();
				info(" root offset for component " + r);

				String signature = td.getSignature();
				info(" signature " + signature);

				/* save component fingerprint for compare */
				if (signature != null && signature.length() > 0)
					tblSig.put(td.getSignature(), td.tblname);

				
				
				tableDescriptorReady(td);

				if (td.isVirtual())
					continue;

				/* transfer component information for later recovery */
				recoveryTables.add(td);

				/* explore a component trees and build up page info */
				exploreBTree(r, td);

			}

			/*******************************************************************/

			for (IndexDescriptor id : indices.values()) {
				int r = id.getRootOffset();
				info(" root offset for index " + r);

				indexDescriptorReady(id);
			}
		
			

			/*******************************************************************/

			/**
			 * Sometimes, a record cannot assigned to a component or index -> these records
			 * are assigned to the __UNASSIGNED component.
			 */

			List<String> col = new ArrayList<String>();
			List<String> names = new ArrayList<String>();

			/* create dummy component for unassigned records */
			for (int i = 0; i < 20; i++) {
				col.add("TEXT");
				names.add("col" + (i + 1));
			}
			TableDescriptor tdefault = new TableDescriptor("__UNASSIGNED", "",col, col, names, null, null, null, false);
			headers.put(tdefault.getName(), tdefault);
			
			unassignedTableCreated(tdefault);
			/*******************************************************************/

			byte freepageno[] = new byte[4];
			buffer.position(36);
			buffer.get(freepageno);
			info("Total number of free list pages " + Auxiliary.bytesToHex(freepageno));
			ByteBuffer no = ByteBuffer.wrap(freepageno);
			fpnumber = no.getInt();
			Logger.out.info(" no " + fpnumber);

			/*******************************************************************/

			byte freelistpage[] = new byte[4];
			buffer.position(32);
			buffer.get(freelistpage);
			info("FreeListPage starts at offset " + Auxiliary.bytesToHex(freelistpage));
			ByteBuffer freelistoffset = ByteBuffer.wrap(freelistpage);
			int head = freelistoffset.getInt();
			info("head:: " + head);
			fphead = head;
			int start = (head - 1) * ps;

			/*******************************************************************/

			if (head == 0) {
				info("INFO: Couldn't locate any free pages to recover. ");
			}

			/*******************************************************************
			 *
			 * STEP 1: we start recovery process with scanning the free list first
			 **/

			if (head > 0) {
				info("first:: " + start + " 0hx " + Integer.toHexString(start));

				long startfp = System.currentTimeMillis();
				Logger.out.info("Start free page recovery .....");

				// seeking file pointer to the first free page entry

				/* create a new threadpool to analyze the freepages */
				ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Global.numberofThreads);

				/* a list can extend over several memory pages. */
				boolean morelistpages = false;

				int freepagesum = 0;

				do {
					/* reserve space for the first/next page of the free list */
					ByteBuffer fplist = file.allocateAndReadBuffer(0, ps);

					// next (possible) freepage list offset or 0xh00000000 + number of entries
					// example : 00 00 15 3C | 00 00 02 2B

					// now read the first 4 Byte to get the offset for the next free page list
					byte nextlistoffset[] = new byte[4];

					try {
                        fplist.get(nextlistoffset);
                    } catch(Exception err) {
                        debug("Warning Error while parsing free list.");
                    }

					/*
					 * is there a further page - <code>nextlistoffset</code> has a value > 0 in this
					 * case
					 */
					if (!Auxiliary.bytesToHex(nextlistoffset).equals(NO_MORE_ENTRIES)) {
						ByteBuffer of = ByteBuffer.wrap(nextlistoffset);
						int nfp = of.getInt();
						start = (nfp - 1) * ps;
						if (!allreadyvisit.contains(nfp))
							allreadyvisit.add(nfp);
						else {
							info("Antiforensiscs found: cyclic freepage list entry");
							morelistpages = true;
						}
					} else
						morelistpages = false;

					// now read the number of entries for this particular page
					byte numberOfEntries[] = new byte[4];
					try {
                        fplist.get(numberOfEntries);
                    } catch(Exception err) {
                    }
					
					ByteBuffer e = ByteBuffer.wrap(numberOfEntries);
					int entries = e.getInt();
					info(" Number of Entries in freepage list " + entries);

					runningTasks.set(0);
					
					/* iterate through free page list and read free page offsets */
					for (int zz = 1; zz <= entries; zz++) {
						byte next[] = new byte[4];
						fplist.position(4 * zz);
						fplist.get(next);

						ByteBuffer bf = ByteBuffer.wrap(next);
						int n = bf.getInt();

						if (n == 0) {
							continue;
						}
						// determine offset for free page
						int offset = (n - 1) * ps;

						//System.out.println("page " + n + " at offset " + offset);

						// if (offset Job.size)
						// continue;

						RecoveryTask task = new RecoveryTask(new Auxiliary(this), this, offset, n, ps, true, recoveryTables);
						/* add new task to executor queue */
						runningTasks.incrementAndGet();
						executor.execute(task);
						// task.run();
					}
					freepagesum += entries;

				} while (morelistpages); // while

				executor.shutdown();

				info("Task total: " + runningTasks.intValue());

				// wait for Threads to finish the tasks
				while (runningTasks.intValue() != 0) {
					try {
						TimeUnit.MILLISECONDS.sleep(10);
						// System.out.println("wait...");
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				info("Number of cells " + numberofcells.intValue());

				info(" Finished. No further free pages. Scanned " + freepagesum);

				long endfp = System.currentTimeMillis();
				info("Duration of free page recovery in ms: " + (endfp - startfp));

			} // end of free page list recovery

			info("Lines after free page recovery: " + ll.size());

			/*******************************************************************/
			// start carving

			// full db-scan (including all database pages)
			scan(numberofpages, ps, recoveryTables);

			/*******************************************************************/
			linesReady();

		} finally {
			closeResources();
		}

		for (Entry<String, TableDescriptor> tableEntry : headers.entrySet()) {
		    List<SqliteRow> rows = tableRows.get(tableEntry.getKey());
		    if (null != rows) {
		        Map<String, Integer> colIdx = new HashMap<>();
		        int id = 0;
		        for (String col : tableEntry.getValue().columnnames) {
		            colIdx.put(col, id++);
		        }

		        for (SqliteRow row : rows) {
		            row.setColumnNamesMap(colIdx);
		        }
		    }
		}


		return 0;
	}

	protected void closeResources() {
	    for (Closeable c : resourcesToClose) {
            try {
                c.close();
            } catch (IOException e) {
                err(e.toString());
            }
        }
	}

	private void readSchema(RandomAccessFileReader file, long index, String headerStr, boolean readWAL) throws IOException
	{
		
		Logger.out.info("Entering readSchema()");
		
		/* we need a utility method for parsing the Mastertable */
		Auxiliary c = new Auxiliary(this);
		
		// compute offset
		int starthere = (int) (index % ps);
		int pagenumber = (int) (index / ps);

		ByteBuffer bbb = null;
		/* first try to read the db schema from the main db file */
		if (!readWAL)
		{
			bbb = readPageWithNumber(pagenumber, ps);
		}
		/* No schema was found? Try reading the WAL file instead */
		else
		{
			long pagebegin = 0;
			// first page ?
			if (index < (ps + 56))
			{
				// skip WAL header (32 Bytes) and frame header (24 Bytes)
				pagebegin = 56;
				starthere  = (int) (index - 56);
			}
			else
			{
			    pagebegin = ((index - 32) / (ps + 24)) * (ps + 24) - 24 + 32;      //32 + (ps+24)*index/(ps+24) + 24;
				starthere = (int) ((index - 32) % (ps + 24) + 24);
			}
			
			/* go to frame start */
			bbb = file.allocateAndReadBuffer(pagebegin, ps);
			
			Logger.out.info("index match " + index);

			Logger.out.info("ReaderWAL is true -> page begin offset " + pagebegin );

			Logger.out.info("ReaderWAL is true -> offset in page " + starthere);
			
			Logger.out.info(" index - starthere - page begin " + (index - starthere - pagebegin));
		}
		
		/* start reading the schema string from the correct position */
		if (db_encoding == StandardCharsets.UTF_8)
			c.readMasterTableRecord(this, starthere - 13, bbb, headerStr);
		else if (db_encoding == StandardCharsets.UTF_16LE)
			c.readMasterTableRecord(this, starthere - 17, bbb, headerStr);
		else if (db_encoding == StandardCharsets.UTF_16BE)
			c.readMasterTableRecord(this, starthere - 18, bbb, headerStr);
	
		Logger.out.info("Leave readSchema()");

	}

	/**
	 *  Translate a given ByteBuffer to a String.
	 *  @return the decoded String
	 *	@param tblb The ByteBuffer to be decoded	 
	 *  
	 */
	public String decode(ByteBuffer tblb) {

		/* Attention: we need to use the correct encoding!!! */
		try {
			byte[] m = tblb.array();

			/* try to read bytes an create a UTF-8 string first */
			String val = new String(m, StandardCharsets.UTF_8);

			/* get byte array with UTF8 representation */
			byte[] n = val.getBytes(StandardCharsets.UTF_8); //org.apache.commons.codec.binary.StringUtils.getBytesUtf8(val);

			/* there are Null-Byte values left after conversion */
			/* I don't know why ? */
		
			byte[] cs = new byte[n.length];

			int xx = 0;
			for (byte e : n) {
				//String ee = String.format("%02X", e);
				//System.out.print(ee + " ");

				if (e != 0) {
					cs[xx] = e;
					xx++;

				}

			}

			String schemastr = new String(cs);

			return schemastr;

		} catch (Exception err) {
			Logger.out.err(err.toString());
		}

		return "";
	}

	/**
	 * The method creates a Task object for each page to be scanned.
	 * Each task is then scheduled into a worker thread's ToDo list. 
	 * 
	 * After all tasks are assigned, the worker threads are started 
	 * and begin processing.  
	 * 
	 * @param number the number
	 * @param ps the ps
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void scan(int number, int ps, List<TableDescriptor> recoveryTables) throws IOException {
		info("Start with scan...");
		/* create a new threadpool to analyze the freepages */
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Global.numberofThreads);

		long begin = System.currentTimeMillis();
		
		/* first start scan of regular pages */
	
		Worker[] worker = new Worker[Global.numberofThreads]; 
        for (int i = 0; i < Global.numberofThreads; i++)
        {
        	worker[i] = new Worker(this);
        }
		
		for (int cc = 1; cc < pages.length; cc++) {
			
			if (null == pages[cc]) 
			{
				debug("page " + cc + " is no regular leaf page component. Maybe a indices or overflow or dropped component page.");
				//System.out.println("page " + cc + " offset " + ((cc-1)*ps) + " is no regular leaf page component. Maybe a indices or overflow or dropped component page.");
			} 
			else 
			{
				debug("page" + cc + " is a regular leaf page. ");

				// determine offset for free page
				long offset = (cc - 1) * ps;

				//System.out.println("************************ pagenumber " + cc + " " + offset);

				RecoveryTask task = new RecoveryTask(worker[cc % Global.numberofThreads].util,this, offset, cc, ps, false, recoveryTables);
				worker[cc % Global.numberofThreads].addTask(task);
				runningTasks.incrementAndGet();
			}
		}
		debug("Task total: " + runningTasks.intValue() + " worker threads " + Global.numberofThreads);
		
		int c = 1;
		/* start executing the work threads */
		for (Worker w : worker)
		{	
			Logger.out.info(" Start worker thread" + c++);
			/* add new task to executor queue */
			//executor.execute(w);			
            w.run();
		}

		try {
		    // System.out.println("attempt to shutdown executor");
		    executor.shutdown();
		
		    int remaining = 0;
		    int i = 0;
		    do
		    {
		    	remaining = runningTasks.intValue();
		    	i++;
		        //System.out.println(" Still running tasks " + remaining);
		        Thread.currentThread();
				Thread.sleep(100);
		    }while(i < 50 && remaining > 0);
		   
		    executor.awaitTermination(2, TimeUnit.SECONDS);
		
		
		}
		catch (InterruptedException e) {
		    System.err.println("tasks interrupted");
		}
		finally {
		    if (!executor.isTerminated()) {
		        System.err.println("cancel non-finished tasks");
		    }
		    executor.shutdownNow();
		    Logger.out.info("shutdown finished");
		}
		
		// wait for Threads to finish the tasks
		while (runningTasks.intValue() != 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(2000);
				Logger.out.info("warte....");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		long ende = System.currentTimeMillis();
		info("Duration of scanning all pages in ms : " + (ende-begin));
		info("End of Scan...");
		
		

	}

	public void setPath(String path) {
		this.path = path;
	}
	
	public void setWALPath(String path) {
		walpath = path;
	}
	
	public void setRollbackJournalPath(String path)
	{
		rollbackjournalpath = path;
	}
	

	/**
	 *  This method is used to start the job. 
	 */
	public void start() {
		if (path != null)
			try {
				run(path);
			} catch (Exception e) {
				e.printStackTrace();
			}
		else
			return;
	}
		
	/**
	 *	 
	 */
	public Job() {
		Base.LOGLEVEL = Global.LOGLEVEL;
	}


	/**
	 * Start processing a new Sqlite file.
	 * 
	 * @param p path to the Sqlite file
	 * @return 0 if successful, -1 if not
	 */
	public int run(String p) {
		
		int hashcode = -1;
		path = p;
		long start = System.currentTimeMillis();
		try {
			hashcode = processDB();
		} catch (InterruptedException | ExecutionException | IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		Logger.out.info("Duration in ms: " + (end - start));

		return hashcode;
	}

	protected String readPageAsString(int pagenumber, int pagesize) throws IOException {

		return Auxiliary.bytesToHex(readPageWithNumber(pagenumber, pagesize).array());

	}

	/**
	 * Read a database page from a given offset with a fixed pagesize.
	 * 
	 * @param offset the offset
	 * @param pagesize the pagesize
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 * @throws IOException if an error occurs while reading the page.
	 */
	public ByteBuffer readPageWithOffset(long offset, int pagesize) throws IOException {

		if ((offset > file.size()) || (offset < 0))
		{
			
			Logger.out.info(" offset greater than file size ?!" + offset + " > " + file.size());
			Auxiliary.printStackTrace();
			
			
			return null;
		}
		return file.allocateAndReadBuffer(offset, pagesize);
	}

	/**
	 *  Since all pages are assigned to a unique number in SQLite, we can read a 
	 *  page by using this value together with the pagesize. 
	 *  
	 * @param pagenumber (&gt;=1)
	 * @param pagesize size of page
	 * @return  A <code>ByteBuffer</code> object containing the page content. 
	 * @throws IOException if an error occurs while reading the page.
	 */
	public ByteBuffer readPageWithNumber(long pagenumber, int pagesize) throws IOException {
		
		if (pagenumber < 0)
		{
		   return null;
		}
		return readPageWithOffset(pagenumber*pagesize, pagesize);
	}
	


	/**
	 * The B-tree , or, more specifically, the B+-tree, 
	 * is the most widely used physical database structure 
	 * for primary and secondary indexes on database relations. 
	 * 
	 * This method can be called to traverse all nodes of 
	 * a table-tree. 
	 * 
	 * Attention! This is a recursive function. 
	 * 
	 * 
	 * @param root  the root node.
	 * @param td   this reference holds a Descriptor for the page type. 
	 * @throws IOException
	 */
	private void exploreBTree(int root, AbstractDescriptor td) throws IOException {
		
		// pagesize * (rootindex - 1) -> go to the start of this page
		int offset = ps * (root - 1);

		if(offset <= 0)
			return;
		
		if (root <  0)
		{
			return;
		}
		
		
		
		// first two bytes of page
		file.position(offset);
		byte pageType = file.get();

		/* check type of the page by reading the first two bytes */
		int typ = Auxiliary.getPageType(Auxiliary.bytesToHex(new byte [] { pageType } ));

		/* not supported yet */
		if (typ == 2) 
		{
			debug(" page number" + root + " is a  INDEXINTERIORPAGE.");
			
		}
		/* type is either a data interior page (12) */
		else if (typ == 12) {
			debug("page number " + root + " is a interior data page ");

			ByteBuffer rightChildptr = file.allocateAndReadBuffer(offset + 8, 4);
			
			/* recursive */
			exploreBTree(rightChildptr.getInt(), td);

			/* now we have to read the cell pointer list with offsets for the other pages */

			/* read the complete internal page into buffer */
			//System.out.println(" read internal page at offset " + ((root-1)*ps));
			ByteBuffer buffer = readPageWithNumber(root - 1, ps);

			byte[] numberofcells = new byte[2];
			buffer.position(3);

			buffer.get(numberofcells);
			ByteBuffer noc = ByteBuffer.wrap(numberofcells);
			int e = Auxiliary.TwoByteBuffertoInt(noc);	
			
			
			byte cpn[] = new byte[2];
			buffer.position(5);

			buffer.get(cpn);

			// ByteBuffer size = ByteBuffer.wrap(cpn);
			// int cp = Auxiliary.TwoByteBuffertoInt(size);

			//System.out.println(" cell offset start: " + cp);
			//System.out.println(" root is " + root + " number of elements: " + e);
			

			/* go on with the cell pointer array */
			for (int i = 0; i < e; i++) {

				// address of the next cell pointer
				byte pointer[] = new byte[2];
				buffer.position(12 + 2 * i);
				if (buffer.capacity() <= buffer.position()+2)
					continue;
				buffer.get(pointer);
				ByteBuffer celladdr = ByteBuffer.wrap(pointer);
				int celloff = Auxiliary.TwoByteBuffertoInt(celladdr);

				debug(" celloff " + celloff);
				// read page number of next node in the btree
				byte pnext[] = new byte[4];
				if (celloff >= buffer.capacity() || celloff < 0)
					continue;
				if (celloff > ps)
					continue;
				buffer.position(celloff);
				buffer.get(pnext);
				int p = ByteBuffer.wrap(pnext).getInt();
				// unfolding the next level of the tree
				debug(" child page " + p);
				exploreBTree(p, td);
			}

		} 
		else if (typ == 8 || typ == 10) {
			debug("page number " + root + " is a leaf page " + " set component/index to " + td.getName());
			if (root >  numberofpages)
				return;
			if (null == pages[root])
				pages[root] = td;
			else
				debug("WARNING page is member in two B+Trees! Possible Antiforensics.");
		} 
		else {
			debug("Page" + root + " is neither a leaf page nor a internal page. Try to set component to " + td.getName());
			
			if (root >  numberofpages)
				return;
			if (null == pages[root])
				pages[root] = td;

		}

	}



	/**
	 * Return the columnnames as a String array for a given table or indextable name.
	 * @param tablename name of the table or index
	 * @return the headerString
	 */
	public String[] getHeaderString(String tablename)
	{
		/* check tables first */
	    TableDescriptor td = headers.get(tablename);
	    if (null != td) {
	        return td.columnnames.toArray(new String[0]);
	    }

		/* check indicies next */
	    IndexDescriptor id = indices.get(tablename);
	    if (null != id) {
	        return id.columnnames.toArray(new String[0]);			
		}

		/* no luck */
		return null;
	}
	
	/**
     * Save findings into a comma separated file.
     * @param filename file to write results to
     * @param lines lines to write
     */
    public void writeResultsToFile(String filename, String [] lines) {
        Logger.out.info("Write results to file...");
        Logger.out.info("Number of records recovered: " + ll.size());

        if (null == filename) {
            Path dbfilename = Paths.get(path);
            String name = dbfilename.getFileName().toString();

            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter df;
            df = DateTimeFormatter.ISO_DATE_TIME; // 2020-01-31T20:07:07.095
            String date = df.format(now);
            date = date.replace(":","_");

            filename = "results" + name + date + ".csv";
        }
        
        Arrays.sort(lines);

        /** convert line to UTF-8 **/
        try {
            
            final File file = new File(filename);
            
            
            try (final BufferedWriter writer = Files.newBufferedWriter(file.toPath(),Charset.forName("UTF-8"), StandardOpenOption.CREATE)) 
            {
              for (String line: lines)
              { 
                  writer.write(line);
              }
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void addRow(SqliteRow row) {
		if (recoverOnlyDeletedRecords) {
			// only add rows that are marked as deleted
			if (!row.isDeletedRow()) {
				return;
			}
		}
        ll.add(row);
        List<SqliteRow> tables = tableRows.get(row.getTableName());
        if (null == tables) {
            tables = Collections.synchronizedList(new ArrayList<>());
            tableRows.put(row.getTableName(), tables);
        }
        tables.add(row);
    }

    public Queue<SqliteRow> getRows() {
        return ll;
    }

    public synchronized List<SqliteRow> getRowsForTable(String tableName) {
        return tableRows.getOrDefault(tableName, Collections.emptyList());
    }

    public synchronized Set<String> getTablesNames() {
        return tableRows.keySet();
    }
}




class Signatures {

	static String getTable(String signature) {

		signature = signature.trim();

		signature = signature.replaceAll("[0-9]", "");

		signature = signature.replaceAll("PRIMARY_KEY", "");

		return signature;
	}
}
