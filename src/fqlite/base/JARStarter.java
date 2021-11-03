package fqlite.base;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This class works as a dispatcher to run the correct main() function.
 * Note: FQLite does support 2 modes: command line interface (cli) and graphical (gui).
 * 
 * To run the FQLite from the command line you can use one of the  following options:
 *
 * $&gt; java -jar  fqliteXXX.jar gui						: start program in gui mode
 * $&gt; java -jar  fqliteXXX.jar nogui &lt;database.db&gt;       : starts the command line version
 * $&gt; java -jar  fqliteXXX.jar cli &lt;database.db&gt;		    : starts the command line version
 * $&gt; java -jar  fqliteXXX.jar                           : start program in gui mode
 * 
 * 
 * @author pawlaszc
 *
 */
public class JARStarter {

   private static Map<String, Class<?>> PROGRAM_MODES = new HashMap<String, Class<?>>();
	   
	    static{
	        PROGRAM_MODES.put("nogui", MAIN.class);
	        PROGRAM_MODES.put("cli", MAIN.class);
	    }
	
	public static void main(String[] args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		
			Class<?> entryPoint = PROGRAM_MODES.get(args[0]);
			if(entryPoint==null){
					MAIN.printOptions();
					return;
			}
			
			final String[] argsCopy = args.length > 1  ? Arrays.copyOfRange(args, 1, args.length) : new String[0];
			entryPoint.getMethod("main", String[].class).invoke(null, (Object) argsCopy);
	}

}
