import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;

public class WimuUtil {
	
	private static int countFiles = 0;
	
	public static void main(String args[]) throws IOException, ParserException{
//		Set<File> setFiles = new HashSet<File>();
//		setFiles.add(new File("/media/andre/DATA/linux/linklion2/wimu_NC11/logs/hdt.txt"));
//		setFiles.add(new File("/media/andre/DATA/linux/linklion2/wimu_NC11/logs/dumps.txt"));
//		setFiles.add(new File("/media/andre/DATA/linux/linklion2/wimu_NC11/logs/endpoints.txt"));
		//String dir = "/media/andre/DATA/linux/linklion2/wimu_NC11/logs/";
		//analyseLogFiles(dir);
		ConcurrentHashMap<String, Integer> mTest = new ConcurrentHashMap<String, Integer>();
		for(int i=0;i<100;i++){
			mTest.put("<http://test.com/nothing> <http://dataset_"+i+".org>", i);
		}
		File f = new File("hdt");
		f.mkdir();
		save2HDT(mTest);
	}
	
	public static void save2HDT(ConcurrentHashMap<String, Integer> mDatatypeTriples) throws IOException, ParserException{
		countFiles++;
		File fNT = map2NT(mDatatypeTriples);
		// Configuration variables
		//String baseURI = "http://example.com/mydataset";
		String baseURI = "http://dice.com/wimu";
		//String rdfInput = "/path/to/dataset.nt";
		String rdfInput = fNT.getAbsolutePath();
		String inputType = "ntriples";
		//String hdtOutput = "/path/to/dataset.hdt";
		String hdtOutput = "hdt/" + fNT.getName().replaceAll(".nt", ".hdt");

		// Create HDT from RDF file
		HDT hdt = HDTManager.generateHDT(rdfInput, baseURI, RDFNotation.parse(inputType), new HDTSpecification(), null);

		try {
			// Save generated HDT to a file
			hdt.saveToHDT(hdtOutput, null);
		} finally {
			// IMPORTANT: Free resources
			hdt.close();
			fNT.delete();
		}
	}
	
	public static File map2NT(ConcurrentHashMap<String, Integer> maps) throws IOException {
		File ret = new File("hdt/" + countFiles + "_temp.nt");
		ret.createNewFile();
		System.out.println("Generating file: " + ret.getAbsolutePath());
		try {
			PrintWriter writer = new PrintWriter(ret.getAbsolutePath(), "UTF-8");
			maps.forEach((uriDataset, dTypes) -> {
				writer.println(uriDataset + " \"" + dTypes + "\"^^<http://www.w3.org/2001/XMLSchema#int> .");
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("File generated: " + ret.getAbsolutePath());
		return ret;
	}

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
	
	public static void analyseLogFiles(String dir) throws IOException {
		File f = new File(dir);
		File logFiles[] = f.listFiles();
		Map<String, String> mapErrors = new HashMap<String, String>();
		Set<String> setSuccess = new HashSet<String>();
		PrintWriter writer = new PrintWriter("LogAnalisis.csv", "UTF-8");
		writer.println("File\tSuccess\tErrors");
		for (File fLog : logFiles) {
			PrintWriter wError = new PrintWriter(fLog.getName() + "_errors.csv", "UTF-8");
			wError.println("Dataset\tError");
			List<String> lstLog = FileUtils.readLines(fLog, "UTF-8");
			String fName = null;
			String error = "";
			for (String line : lstLog) {
				if (line.startsWith("SUCESS: ")) {
					fName = line.split("SUCESS: ")[1];
					setSuccess.add(fName.trim());
					mapErrors.put(fName.trim(),error);
				} else if (line.startsWith("FAIL: ")) {
					fName = line.split("FAIL: ")[1];
					String s[] = fName.split(" ERROR:");
					fName = s[0];
					error = s[1].trim();
					mapErrors.put(fName.trim(),error);
				}
			}
			mapErrors.entrySet().forEach(entry ->{
				wError.println(entry.getKey() + "\t" + entry.getValue());
			});
			wError.close();
			writer.println(fLog.getName() + "\t" + setSuccess.size() + "\t" + mapErrors.size());
		}
		writer.close();
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
	
	public static void mergeLuceneIndex(String dir) throws IOException {
		final Version LUCENE_VERSION = Version.LUCENE_44;

		File INDEXES_DIR = new File(dir);

		Date start = new Date();

		File indexDirectory = new File("indexLuceneDir");
		indexDirectory.mkdir();
		MMapDirectory directory = new MMapDirectory(indexDirectory);
		System.out.println("Created Lucene dir: " + indexDirectory.getAbsolutePath());
		Analyzer urlAnalyzer = new SimpleAnalyzer(LUCENE_VERSION);
		Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
		mapping.put("uri", urlAnalyzer);
		mapping.put("dataset_dtype", urlAnalyzer);
		PerFieldAnalyzerWrapper perFieldAnalyzer = new PerFieldAnalyzerWrapper(urlAnalyzer, mapping);
		IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, perFieldAnalyzer);
		IndexWriter iwriter = new IndexWriter(directory, config);
		iwriter.commit();

		FSDirectory indexes[] = new FSDirectory[INDEXES_DIR.list().length];

		for (int i = 0; i < INDEXES_DIR.list().length; i++) {
			System.out.println("Adding: " + INDEXES_DIR.list()[i]);
			File f = new File(INDEXES_DIR.getAbsolutePath() + "/" + INDEXES_DIR.list()[i]);
			indexes[i] = FSDirectory.open(f);
			System.out.println(indexes[i]);
		}

		System.out.print("Merging added indexes...");
		iwriter.addIndexes(indexes);
		System.out.println("done");

		iwriter.close();
		System.out.println("done");

		Date end = new Date();
		System.out.println("It took: " + ((end.getTime() - start.getTime()) / 1000) + "\"");
	}

}
