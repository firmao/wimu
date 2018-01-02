import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;

public class WimuUtil {
	public static Set<String> getAlreadyProcessed(String logFileName) throws IOException {
		Set<String> setReturn = new HashSet<String>();
		if (logFileName == null)
			return setReturn;
		File f = new File(logFileName);
		if (!f.exists()) {
			System.out.println("File: " + logFileName + " does not exist yet, creating now.");
			return setReturn;
		}

		List<String> lstLog = FileUtils.readLines(f, "UTF-8");
		String fName = null;
		for (String line : lstLog) {
			if (line.startsWith("SUCESS: ")) {
				fName = line.split("SUCESS: ")[1];
				setReturn.add(fName.trim());
			} else if (line.startsWith("FAIL: ")) {
				fName = line.split("FAIL: ")[1];
				fName = fName.split(" ERROR:")[0];
				setReturn.add(fName.trim());
			}
		}
		System.out.println("skiping " + setReturn.size() + " already processed.");
		return setReturn;
	}
	
	public static Set<String> skipWrongFiles(String logFileName) throws IOException {
		Set<String> setReturn = new HashSet<String>();
		if (logFileName == null)
			return setReturn;
		File f = new File(logFileName);
		if (!f.exists()) {
			System.out.println("File: " + logFileName + " does not exist yet, creating now.");
			return setReturn;
		}

		List<String> lstLog = FileUtils.readLines(f, "UTF-8");
		String fName = null;
		for (String line : lstLog) {
			if (line.startsWith("FAIL: ")) {
				fName = line.split("FAIL: ")[1];
				fName = fName.split(" ERROR:")[0];
				setReturn.add(fName.trim());
			}
		}
		System.out.println("skiping " + setReturn.size() + " ERROR files already processed.");
		return setReturn;
	}
	
	public static void printMemory(){
		/* Total amount of free memory available to the JVM */
	    System.out.println("Free memory (bytes): " + 
	        Runtime.getRuntime().freeMemory());

	    /* This will return Long.MAX_VALUE if there is no preset limit */
	    long maxMemory = Runtime.getRuntime().maxMemory();
	    /* Maximum amount of memory the JVM will attempt to use */
	    System.out.println("Maximum memory (bytes): " + 
	        (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

	    /* Total memory currently available to the JVM */
	    System.out.println("Total memory available to JVM (bytes): " + 
	        Runtime.getRuntime().totalMemory());
	}
}
