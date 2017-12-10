import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;

public class WimuUtil {
	public static Set<String> getAlreadyProcessed(String logFileName) throws IOException {
		Set<String> setReturn = new HashSet<String>();
		File f = new File(logFileName);
		if(!f.exists()){
			System.out.println("File: " + logFileName + " does not exist yet, creating now.");
			return setReturn;
		}
			
		List<String> lstLog = FileUtils.readLines(f, "UTF-8");
		String fName = null;
		for (String line : lstLog) {
			if(line.startsWith("SUCESS: ")){
				fName = line.split("SUCESS: ")[1];
				setReturn.add(fName);
			}else if(line.startsWith("FAIL: ")){
				fName = line.split("FAIL: ")[1];
				fName = fName.split(" ERROR:")[0];
				setReturn.add(fName);
			}
		}
		System.out.println("skiping " + setReturn.size() + " already processed.");
		return setReturn;
	}
}
