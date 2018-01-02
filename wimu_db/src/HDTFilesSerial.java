import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

public class HDTFilesSerial {
	public static Set<String> alreadyProcessed = new HashSet<String>();
	public static Set<String> errorFiles = new HashSet<String>();
	public static Set<String> successFiles = new HashSet<String>();
	public static Set<String> setFileURLs = new HashSet<String>();
	public static ConcurrentHashMap<String, String> datasetErrorsJena = new ConcurrentHashMap<String, String>();
	//public static ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
	// public static Set<String> setDataTypes = new HashSet<String>();

	// Lucene
	public static final String N_TRIPLES = "NTriples";
	public static final String TTL = "ttl";
	public static final String TSV = "tsv";
	public static final Version LUCENE_VERSION = Version.LUCENE_44;
	private static Analyzer urlAnalyzer;
	private static DirectoryReader ireader;
	private static IndexWriter iwriter;
	private static MMapDirectory directory;

	public static long count = 0;
	public static long countDataTypeTriples = 0;
	public static long totalTriples = 0;
	public static long countFile = 0;
	// public static long lim = 30000000;
	public static long lim = 100000;
	public static long origLim = 0;
	public static long start = 0;
	public static long totalTime = 0;
	public static String fDatypeName = null;
	public static Dataset datasetJena = null;
	private static String dumpDir;
	private static String luceneDir;

	public static void main(String args[]) throws MalformedURLException, IOException {
		//create("dumpDir", "luceneDir");
		
		//test getFiles
		Set<String> setNameFiles = new HashSet<String>();
		setNameFiles.add("http://download.lodlaundromat.org/85d5a476b56fde200e770cefa0e5033c?type=hdt");
		getFiles(4, setNameFiles);
	}

	public static void create(String pDumpDir, String pLuceneDir) throws IOException {

		dumpDir = pDumpDir;
		luceneDir = pLuceneDir;

		File f = new File("out");
		if (!f.exists())
			f.mkdir();

		File f1 = new File(dumpDir);
		if (!f1.exists())
			f1.mkdir();

		File f2 = new File("unzip");
		if (!f2.exists())
			f2.mkdir();

		System.out.println("Dumps dir: " + f1.getAbsolutePath());

		File indexDirectory = new File(luceneDir);
		indexDirectory.mkdir();
		directory = new MMapDirectory(indexDirectory);
		System.out.println("Created Lucene dir: " + indexDirectory.getAbsolutePath());
		urlAnalyzer = new SimpleAnalyzer(LUCENE_VERSION);
		Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
		mapping.put("uri", urlAnalyzer);
		mapping.put("dataset_dtype", urlAnalyzer);
		PerFieldAnalyzerWrapper perFieldAnalyzer = new PerFieldAnalyzerWrapper(urlAnalyzer, mapping);
		IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, perFieldAnalyzer);
		iwriter = new IndexWriter(directory, config);

		//iwriter.deleteAll();
		//
		iwriter.commit();

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("HDT2Lucene...Serial, starting: " + dateFormat.format(date));
		start = System.currentTimeMillis();

		int cores = Runtime.getRuntime().availableProcessors();
		alreadyProcessed.addAll(WimuUtil.getAlreadyProcessed(Wimu.logFileName));

		Set<String> setAllFileURLs = getDumps("md5HDT.txt");
		setAllFileURLs.removeAll(alreadyProcessed);

		setFileURLs.addAll(setAllFileURLs);
		// setAllFileServer.forEach(fileName -> {
		for (String fileURL : setAllFileURLs) {
			if (alreadyProcessed.contains(fileURL))
				continue;

			Set<FileWIMU> setFiles = null;
			try {
				long tDownload = System.currentTimeMillis();
				setFileURLs.removeAll(alreadyProcessed);
				System.out.println("Starting downloading " + cores + " Files. Already processed files: "
						+ alreadyProcessed.size() + " from " + setAllFileURLs.size());
				setFiles = getFiles(cores, setFileURLs);
				long totalTDownload = System.currentTimeMillis() - tDownload;
				System.out.println("Total TimeDownload(ms): " + totalTDownload);
			} catch (IOException e) {
				datasetErrorsJena.put(fileURL, e.getMessage());
			}
			System.out.println("Starting to process " + cores + " threads/files. Already processed files: "
					+ alreadyProcessed.size() + " from " + setAllFileURLs.size());

			long tProcess = System.currentTimeMillis();
			System.out.println("Serial processing##");
			//setFiles.parallelStream().forEach(file -> {
			for (FileWIMU file : setFiles) {
				String provenance = file.getDataset();
				if (processHDT(file)) {
					synchronized (successFiles) {
						successFiles.add(provenance);
					}
					System.out.println("SUCESS: " + provenance);
				} else {
					System.out.println("FAIL: " + provenance + " ERROR: " + datasetErrorsJena.get(provenance));
				}

				alreadyProcessed.add(provenance);

			}
			//});
			long totalTProcess = System.currentTimeMillis() - tProcess;
			System.out.println("Total TimeProcess(" + cores + " files): " + totalTProcess);
			//System.out.println("mDatatypes.size: " + mDatatypeTriples.size());
		}
		//System.out.println("Inserting remaining " + mDatatypeTriples.size() + " Datatypes");
		//insertLucene(mDatatypeTriples);
		generateFile(datasetErrorsJena, "ErrorsJena.csv");
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total Time(ms): " + totalTime);
		System.out.println("countDataTypeTriples: " + countDataTypeTriples);
		date = new Date();
		System.out.println("HDTSerial..., finalize at: " + dateFormat.format(date));
		if (iwriter != null) {
			iwriter.close();
		}
		if (ireader != null) {
			ireader.close();
		}
		if (directory != null) {
			directory.close();
		}
	}

	private static boolean processHDT(FileWIMU file) {
		System.out.println("processing File: " + file.getAbsolutePath());
		boolean ret = false;
		ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
		int cDtypes = 0;
		String uriDataset = null;
		
		HDT hdt;
		try {
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			String sparql = "Select * where {?s ?p ?o . filter(isLiteral(?o)) }";

			Query query = QueryFactory.create(sparql);

			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			while (results.hasNext()) {
				QuerySolution thisRow = results.next();
				uriDataset = thisRow.get("s").toString() + "\t" + file.getDataset();
				if (mDatatypeTriples.containsKey(uriDataset)) {
					cDtypes = mDatatypeTriples.get(uriDataset) + 1;
					mDatatypeTriples.put(uriDataset, cDtypes);
				} else {
					mDatatypeTriples.put(uriDataset, 1);
				}
				++totalTriples;
			}
			qe.close();
			insertLucene(mDatatypeTriples);
			System.out.println("Total processed datatype triples until the moment: " + totalTriples); 
			file.delete();
			ret = true;
		} catch (Exception e) {
			ret = false;
		} finally {
			
		}
		return ret;
	}

	private static void insertLucene(ConcurrentHashMap<String, Integer> mDatatypeTriples2) throws IOException {
		System.out.println("Insert Lucene. mappingCount mDatatypeTriples: " + mDatatypeTriples2.mappingCount());
		// mDatatypeTriples2.forEach((uriDs, dTypes) -> {
		mDatatypeTriples2.entrySet().forEach(elem -> {
			String uriDs = elem.getKey();
			int dTypes = elem.getValue();

			String s[] = uriDs.split("\t");
			String uri = s[0];
			String dataset = s[1];
			try {
				Document doc = new Document();
				doc.add(new StringField("uri", uri, Store.YES));
				doc.add(new StringField("dataset_dtype", dataset + "\t" + dTypes, Store.YES));
				iwriter.addDocument(doc);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		});
		// iwriter.close();
	}

	private static Set<String> getDumps(String fileName) throws IOException {
		Set<String> setDumps = Files.lines(Paths.get(fileName)).collect(Collectors.toSet());
		Set<String> ret = new HashSet<String>();
		for (String md5 : setDumps) {
			String dump = "http://download.lodlaundromat.org/" + md5 + "?type=hdt";
			if (isGoodURL(dump)) {
				ret.add(dump);
			} else {
				System.out.println("FAIL: " + dump + " ERROR: Invalid URI, not possible to download the content.");
			}
		}
		return ret;
	}

	private static Set<FileWIMU> getFiles(int cores, Set<String> setFileServer) throws IOException {
		Set<FileWIMU> ret = new HashSet<FileWIMU>();
		count = 0;
		// setAllFileNames.removeAll(alreadyProcessed);
		// setFileServer.parallelStream().limit(cores).forEach(sFileURL -> {
		System.out.println("Download Files_");
		for (String sFileURL : setFileServer) {
			if (count == cores)
				break;
			// setFileServer.parallelStream().limit(cores).forEach(sFileURL -> {
			// setFileServer.stream().limit(cores).forEach(sFileURL -> {
			try {
				// if(!isGoodURL(sFileURL)) {
				// alreadyProcessed.add(sFileURL);
				// System.out.println("FAIL: " + sFileURL + " ERROR: Invalid
				// URI, not possible to download the content.");
				// throw new Exception("Invalid URI, not possible to download
				// the content.");
				// }
				String sFileName = getURLFileName(sFileURL);
				FileWIMU fRet = new FileWIMU(dumpDir + "/" + sFileName);
				URL url = new URL(sFileURL);
				FileUtils.copyURLToFile(url, fRet);
				fRet.setDataset(sFileURL);
				ret.add(fRet);
				count++;
				System.out.println(count + " : " + sFileURL);
			} catch (Exception ex) {
				if (datasetErrorsJena.mappingCount() > lim) {
					generateFile(datasetErrorsJena, "ErrorsJena_" + totalTriples + ".csv");
					datasetErrorsJena.clear();
					datasetErrorsJena = new ConcurrentHashMap<String, String>();
				}
				datasetErrorsJena.put(sFileURL, ex.getMessage());
				// ex.printStackTrace();
			}
		}
		// });
		return ret;
	}

	private static boolean isGoodURL(String url) {
		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setRequestMethod("HEAD");
			int responseCode = connection.getResponseCode();
			if ((responseCode == 200) || (responseCode == 400)) {
				return true;
			} else
				return false;
		} catch (Exception e) {
			return false;
		}
	}

	public static File generateFile(Map<String, String> endPointErrors, String fileName) {
		File ret = new File("out/" + fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			endPointErrors.forEach((endPoint, error) -> {
				writer.println(endPoint + "\t" + error);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}

	public static String getURLFileName(URL pURL) {
		String[] str = pURL.getFile().split("/");
		return str[str.length - 1];
	}

	public static String getURLFileName(String pURL) {
		String ret = null;
		String[] str = pURL.split("/");
		ret = str[str.length - 1];
		if (ret.indexOf('?') > 0)
			ret = ret.split("\\?")[0];
		
		if(pURL.endsWith("=hdt"))
			ret += ".hdt";
			
		File f = new File(dumpDir + "/" + ret);
		if (f.exists())
			ret = ret.replace(".", "_" + (countFile++) + ".");

		return ret;
	}
}
