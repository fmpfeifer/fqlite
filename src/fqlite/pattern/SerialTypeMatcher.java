package fqlite.pattern;

import java.nio.ByteBuffer;

import fqlite.util.Auxiliary;
import fqlite.util.BufferUtil;

/**
 * An engine that performs match operations on a byte buffer sequence by
 * interpreting a pattern.
 * 
 * @author pawlaszc
 *
 */
public class SerialTypeMatcher {

	HeaderPattern pattern = null;
	ByteBuffer buffer = null;
	int pos = 0;
	int startRegion;
	int endRegion;
	int start;
	int end;
	MatchingMode mode = MatchingMode.NORMAL;

	/**
	 * Constructor.
	 * 
	 * @param buffer ByteBuffer to analyze
	 */
	public SerialTypeMatcher(ByteBuffer buffer) {
		this.buffer = buffer;
		startRegion = 0;
		endRegion = buffer.capacity();
		buffer.position(0);
	}

	/**
	 * Change the matching behavior. You can choose between:
	 * NORMAL,NOHEADER,NO1STCOL.
	 * 
	 * @param newMode the matching mode
	 */
	public void setMatchingMode(MatchingMode newMode) {
		mode = newMode;
	}

	public MatchingMode getMachtingMode() {
		return mode;
	}

	/**
	 * Set a list of matching constrains.
	 * 
	 * @param pattern the pattern
	 */
	public void setPattern(HeaderPattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Sets the limits of this matcher's region.
	 * 
	 * @param from the start of the region
	 * @param to the end of the region
	 */
	public void region(int from, int to) {
		this.startRegion = from;
		this.endRegion = to;
		// renew position for search
		buffer.position(from);
	}

	/**
	 * Returns the offset after the last character matched.
	 * 
	 * @return The offset after the last character matched
	 */
	public int end() {
		return end;
	}

	/**
	 * Returns the start indices of the previous match.
	 *
	 * @return The indices of the first byte matched
	 */
	public int start() {
		return start;
	}

	/**
	 * This method starts at the beginning of this matcher's region, or, if a
	 * previous invocation of the method was successful and the matcher has not
	 * since been reset, at the first character not matched by the previous match.
	 * If the match succeeds then more information can be obtained via the start,
	 * end, and group methods.
	 *
	 * @return true if, and only if, a subsequence of the input sequence matches
	 *         this matcher's pattern
	 */
	public boolean find() {
		

		int idx = 0;
		switch (mode) {
		case NORMAL:
			idx = 0;
			break;

		case NOHEADER:
			idx = 1;
			break;

		case NO1stCOL:
			idx = 2;
			break;
		}
		int i = idx;

		/* check pattern constrain by constrain */
  
		if (i < pattern.size()) {
    		while (i < pattern.size()) {
    			/* do not read out of bounds - stop before the end */
    			if (buffer.position() < (endRegion - 4)) {
    				/* remember the begin of a possible match */
    				int current = buffer.position();
    				if (i == idx)
    					pos = current;
    				/* read next value */
    				int value = readUnsignedVarInt();
    				// no varint OR costrain does not match -> skip this an go on with the next
    				// bytes
    				if (value == -1 || !pattern.get(i).match(value)) {
    					current++;
    					buffer.position(current);
    					/* and again, startRegion the beginning but with the next byte */
    					i = idx;
    					/* skip pattern matching step and try again */
    					continue;
    				}
    
    				/* go ahead with next constrain */
    				i++;
    
    			} else
    				return false; /* no match could be found */
    
    		}
		} else {
		    // move position forward, as start index is bigger than pattern size
		    pos = buffer.position() + 1;
		    return false;
		}

		start = pos;
		end = buffer.position();
		if (end <= start)
			return false;
		
		return true; // byte number of the match
	}

	/**
	 * Returns the input subsequence matched by the previous match. For a matcher m
	 * with input sequence s, the expressions m.group() and s.substring(m.start(),
	 * m.end()) are equivalent.
	 * 
	 * @return The (possibly empty) subsequence matched by the previous match
	 */
	public ByteBuffer group() {
		byte[] match = BufferUtil.allocateByteBuffer((end) - start);
		buffer.position(start);
		buffer.get(match, 0, (end - start));
		return ByteBuffer.wrap(match);
	}

	/**
	 * Returns the input subsequence matched by the previous match. The return value
	 * contains a hex-representation of the match byte values.
	 * 
	 * @param start the start index of the match
	 * @param end the end index of the match
	 * @return the matched substring
	 */
	public String substring(int start, int end) {
		if (start > end)
			return "";
		byte[] match = BufferUtil.allocateByteBuffer((end) - start);
		buffer.position(start);
		buffer.get(match, 0, (end - start));
		return Auxiliary.bytesToHex(match);
	}

	/**
	 * Returns the input subsequence matched by the previous match. The return value
	 * contains a hex-representation of the match byte values.
	 * 
	 * @return hex representation of matched group
	 */
	public String group2Hex() {
		return substring(start, end);
	}

	/**
	 * Try to read the next varint value from the buffer.
	 * 
	 * @return the corresponding int value or -1 if no varint could be found.
	 */
	public int readUnsignedVarInt() {
		int value = 0;
		int b = 0;
		int counter = 0;
		int shift = 0;

		// as long as we have a byte with most significant bit value 1
		// there are more byte to read
		// we only read a maximum of 3 bytes (8 bytes would be possible to)
		while ((((b = buffer.get()) & 0x80) != 0) && counter < 3) {
			counter++;
			shift += 7;
			value |= (b & 0x7F) << shift;
		}
		// last rightmost byte has always need to have a 0 at the MSB
		// hence, if there is a 1 -> no varint value
		if ((b & 0x80) != 0)
			return -1;

		// return a normalized integer value
		return value | b;
	}

	

}
