package fqlite.util;


import java.io.IOException;
import java.util.Arrays;

/**
 * An efficient byte buffer searching class based on the Knuth-Morris-Pratt algorithm.
 * For more on the algorithm works see: 
 * http://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm.
 */
public class ByteSeqSearcher{


	    private byte[] pattern_;
	    private int[] borders_;

	    // An upper bound on pattern length for searching. Results are undefined for longer patterns.
	    public static final int MAX_PATTERN_LENGTH = 2048;

	    public ByteSeqSearcher(byte[] pattern)
	    {
	        setPattern(pattern);
	    }

	    /**
	     * Sets a new pattern for this.
	     *
	     * @param pattern the pattern we will look for in future calls to search(...)
	     */
	    public void setPattern(byte[] pattern)
	    {
	        pattern_ = Arrays.copyOf(pattern, pattern.length);
	        borders_ = new int[pattern_.length + 1];
	        preProcess();
	    }
	
	    /**
	     * Searches for the next occurrence of the pattern in the buffer
	     * starting startRegion the given position
		 * @param file the FileReader
		 * @param start the start position
		 * @return the position of the next occurrence of the pattern or -1 if not found
	     * @throws IOException if an I/O error occurs
	     */
	     public long indexOf(RandomAccessFileReader file, long start) throws IOException
	     {
	         file.position(start);
	         return indexOf(file);
	     }

	    
	    /**
	     * Searches for the next occurrence of the pattern in the buffer, starting startRegion the current buffer position. Note
	     * that the position of the buffer is changed. If a match is found, the buffer points to the end of the match -- i.e. the
	     * byte AFTER the pattern. Else, the buffer is entirely consumed. The latter is because Inputbuffer semantics make it difficult to have
	     * another reasonable default, i.e. leave the buffer unchanged.
	     *
		 * @param file the fileReader
	     * @return bytes consumed if found, -1 otherwise.
		 * @throws IOException if an I/O error occurs
	     */
	    public long indexOf(RandomAccessFileReader file) throws IOException
	    {
	    	//int size = buffer.capacity() - buffer.position();
	        
	        int b;
	        int j = 0;

	        while (file.position() < file.size())
	        {
	            b = file.get();
	         
	            while (j >= 0 && (byte) b != pattern_[j])
	            {
	                j = borders_[j];
	            }
	            // Move to the next character in the pattern.
	            ++j;

	            // If we've matched up to the full pattern length, we found it.  Return,
	            // which will automatically save our position in the Input-Buffer at the point immediately
	            // following the pattern match.
	            if (j == pattern_.length)
	            {
	                return file.position();
	            }
	        }

	        // No dice, Note that the buffer is now completely consumed.
	        return -1;
	    }

	    /**
	     * Builds up a component of longest "borders" for each prefix of the pattern to find. 
	     * This component is stored internally and aids in implementation of the Knuth-Moore-Pratt 
	     * string search.
	     * <p>
	     * For more information, see: http://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm.
	     * 
	     */
	    private void preProcess()
	    {
	        int i = 0;
	        int j = -1;
	        borders_[i] = j;
	        while (i < pattern_.length)
	        {
	            while (j >= 0 && pattern_[i] != pattern_[j])
	            {
	                j = borders_[j];
	            }
	            borders_[++i] = ++j;
	        }
	    }
	    
	  
}
