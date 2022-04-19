package fqlite.base;

import java.nio.ByteBuffer;
import java.util.BitSet;
import fqlite.descriptor.TableDescriptor;
import fqlite.pattern.MatchingMode;
import fqlite.pattern.SerialTypeMatcher;
import fqlite.types.CarverTypes;
import fqlite.util.Auxiliary;
import fqlite.util.CarvingResult;

/**
 * Use this class to carve for deleted records inside a page.
 * 
 * We can search for header signatures of records either within 
 * the whole page or only in a specific area.
 * 
 * @author pawlaszc
 *
 */
public class Carver extends Base {

	ByteBuffer block;

	BitSet bs;

	String content;

	Job job;

	int pagenumber;

	/**
	 * Constructor.
	 * 
	 * @param job    reference to the calling job object
	 * @param bl         a ByteBuffer representing the binary page content
	 * @param content    the String representation of the page content
	 * @param bs         a BitSet to mark places
	 * @param pagenumber the number of the page within the database.
	 */
	public Carver(Job job, ByteBuffer bl, String content, BitSet bs, int pagenumber) {
		this.job = job;
		block = bl;
		this.bs = bs;
		this.content = content;
		this.pagenumber = pagenumber;
	}

	/**
	 * This method allows to carve for hidden records in the slack space (uncharted
	 * region: a region that does not belong to the header or to the cell content
	 * region).
	 * @param fromidx the index to start carving from
	 * @param toidx  the index to stop carving at
	 * @param mat   the matcher to use
	 * @param headertype the type of the header to search for
	 * @param tbd  the TableDescriptor
	 * @param firstcol to append the result to
	 * @return if any record was carved
	 */
	public int carve(int fromidx, int toidx, SerialTypeMatcher mat, int headertype, TableDescriptor tbd,
			StringBuffer firstcol) {
		Auxiliary c = new Auxiliary(job);

		switch (headertype) {
		case CarverTypes.NORMAL:
			mat.setMatchingMode(MatchingMode.NORMAL);
			break;
		case CarverTypes.COLUMNSONLY:
			mat.setMatchingMode(MatchingMode.NOHEADER);
			break;
		case CarverTypes.FIRSTCOLUMNMISSING:
			mat.setMatchingMode(MatchingMode.NO1stCOL);
			break;
		}
		

		if((toidx - fromidx) <= 4)
			return -1;
		
		/* set search region */
		mat.region(fromidx, toidx);

		/* set pattern to search for */
		mat.setPattern(tbd.getHpattern());

		/* find every match within the given region */
		while (mat.find()){
			
			/* get the hex-presentation of the match */
			String m = mat.group2Hex();
			/* skip stupid matches - remember - it is just a heuristic */
			if ((m.length() < 2) || (m.startsWith("00000000")))
				  continue;
			//System.out.println("Breich: " + ((pagenumber - 1) * job.ps + fromidx) + " "
			//		+ ((pagenumber - 1) * job.ps + toidx));

			/* get the start indices of the match */
			int from = mat.start();

			/* region visited in a match before */
			if (bs.get(from)) {
				continue;
			}

			/*
			 * get the indices of the 1st byte after the header -> this is, where the data
			 * begins
			 */
			int end = mat.end();

			debug("Match (0..NORMAL, 1..NOLENGTH, 2..FIRSTCOLMISSING) : " + headertype);
			debug("found " + m);
			debug("Match: " + m + " on pos:" + ((pagenumber - 1) * job.ps + from));
			
			boolean missing = false;
			if (headertype == CarverTypes.NORMAL) {
			    if (firstcol.length() >= 4 && m.length() >= 4) {
			        firstcol.insert(0,m.substring(2, 4));
			    } else {
			        missing = true;
			    }
				//System.out.println(" Adding knownfirstcolumntypes " + m.substring(2, 4));
			}

			if (headertype == CarverTypes.COLUMNSONLY && m.length() >= 2) {
				firstcol.insert(0,m.substring(0, 2));
				//System.out.println(" Adding knownfirstcolumntypes " + m.substring(0, 2));

				m = addHeaderByte(m);
			}

			if (headertype == CarverTypes.FIRSTCOLUMNMISSING || missing) {
				if (null != firstcol && firstcol.length()>2 && !firstcol.subSequence(0,2).equals("00")) {
					m = firstcol.substring(0,2) + m; 
		
				} 
				else {
				    /* is the first column a integer colum or something else?*/
                    if (tbd.primarykeycolumns != null) {
                      m = "00" + m;
                    } else {
                      m = "02" + m;
                    }
				}
					

				m = addHeaderByte(m);
			}

			if (true) {
				try {
					
					if (pagenumber >  job.numberofpages)
						continue;
					
					CarvingResult res = c.readDeletedRecord(job, end, block, m, bs, pagenumber);

					if (null == res)
					{
						/* something went wrong */
						continue;
					}

					SqliteRow row = res.row;

					if (headertype > 1 && (res.rcursor + 4 <= toidx)) {
						mat.region(res.rcursor + 4, toidx);
					}

					// add new line to output
					if (null != row) { // && rc.length() > 0) {
					    row.setTableName(tbd.tblname);
					    row.setRecordType(Global.DELETED_RECORD_IN_PAGE);
					    job.addRow(row);
					}

				} catch (Exception err) {
					warning("Could not read record" + err.toString());
					return -1;
				}
			}

		}

		return 0;
	}

	private String addHeaderByte(String s) {
		int hl = (s.length() / 2) + 1;

		String hls = Integer.toHexString(hl); // Job.Int2Hex(hl);

		if (hls.length() == 1)
			hls = "0" + hls;

		return hls + s;
	}


}
