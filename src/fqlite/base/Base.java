package fqlite.base;

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
	public static int LOGLEVEL = ALL;


	public void debug(String message) {

		if (LOGLEVEL <= DEBUG)
			System.out.println("[DEBUG] " +message);
	}


    public void info(String message) {

		if (LOGLEVEL <= INFO)
			System.out.println("[INFO] " + message);
	}

	public void warning(String message) {

		if (LOGLEVEL <= WARNING)
			System.out.println("[WARNING] " + message);
	}

	public void err(String message) {

		/* always print error messages */
		System.err.println("ERROR: " + message);
	}

	/*
     * The idea behind these method is to only call toString()
     * in messageObject if it is to be logger, otherwise the
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
