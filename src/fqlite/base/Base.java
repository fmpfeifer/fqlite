package fqlite.base;

import java.nio.ByteBuffer;

import fqlite.util.Auxiliary;

/**
 * Defines an abstracts Base class with some basic
 * logging functionality.
 * 
 * @author pawlaszc
 *
 */
public abstract class Base {

	public static final int ALL = 0;
	public static final int DEBUG = 1;
	public static final int INFO = 2;
	public static final int WARNING = 3;
	public static final int ERROR = 4;
	public static final int NONE = 5;
	public static int LOGLEVEL = ALL;


	public void debug(String message) {
	    out("[DEBUG] ", message, DEBUG);
	}
	
	public void debug(Object... message) {
	    out("[DEBUG] ", message, DEBUG);
    }


    public void info(String message) {
        out("[INFO] ", message, INFO);
	}
    
    public void info(Object... message) {
        out("[INFO] ", message, INFO);
    }

	public void warning(String message) {
	    out("[WARNING] ", message, WARNING);
	}
	
	public void warning(Object... message) {
        out("[WARNING] ", message, WARNING);
    }

	public void err(String message) {
	    out("[ERROR] ", message, ERROR);
	}
	
	public void err(Object... message) {
        out("[ERROR] ", message, ERROR);
    }
	
	private String objectArrayToMessage(Object[] message) {
	    StringBuilder builder = new StringBuilder(message.length);
	    for (Object o : message) {
	        if (null == o) {
	            builder.append("null");
	        } else {
	            if (o instanceof byte[]) {
	                builder.append(Auxiliary.bytesToHex((byte[]) o));
	            } else if (o instanceof ByteBuffer) {
	                builder.append(Auxiliary.bytesToHex((ByteBuffer) o));
	            } else {
	                builder.append(o.toString());
	            }
	        }
	    }
	    return builder.toString();
	}
	
	private void out(String prefix, String message, int level) {
	    if (LOGLEVEL <= level) {
	        System.out.println(prefix + message);
	    }
	}
	
	private void out(String prefix, Object[] message, int level) {
	    if (LOGLEVEL <= level) {
            System.out.println(prefix + objectArrayToMessage(message));
        }
    }

	/*
     * The idea behind these method is to only call toString()
     * in messageObject if it is to be logged, otherwise the
     * potentially expensive String generation is bypassed
     */
    public void debug(Object messageObject) {
        if (LOGLEVEL <= DEBUG) {
            debug(messageObject.toString());
        }
    }

    public void info(Object messageObject) {
        if (LOGLEVEL <= INFO) {
            info(messageObject.toString());
        }
    }

    public void warning(Object messageObject) {
        if (LOGLEVEL <= WARNING) {
            warning(messageObject.toString());
        }
    }
}
