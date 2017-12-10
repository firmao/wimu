import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

public class Endpoint2Lucene {

	private static long totalTriples = 0;
	static ConcurrentHashMap<String, String> mEndPointError = new ConcurrentHashMap<String, String>();

	// Lucene
	public static final String N_TRIPLES = "NTriples";
	public static final String TTL = "ttl";
	public static final String TSV = "tsv";
	public static final Version LUCENE_VERSION = Version.LUCENE_44;
	private static Analyzer urlAnalyzer;
	private static IndexWriter iwriter;
	private static MMapDirectory directory;
	private static String luceneDir;
	
	public static long start = 0;
	public static long totalTime = 0;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		start = System.currentTimeMillis();
		processEndPoints();
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime + " TotalTriples: " + totalTriples);
	}

	public static void create(String pLuceneDir, boolean forceSameDir) throws IOException {
		luceneDir = pLuceneDir;
		File indexDirectory = new File(luceneDir);
		if(!forceSameDir) indexDirectory.mkdir();
		directory = new MMapDirectory(indexDirectory);
		System.out.println("Created Lucene dir: " + indexDirectory.getAbsolutePath());
		urlAnalyzer = new SimpleAnalyzer(LUCENE_VERSION);
		Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
		mapping.put("uri", urlAnalyzer);
		mapping.put("dataset_dtype", urlAnalyzer);
		PerFieldAnalyzerWrapper perFieldAnalyzer = new PerFieldAnalyzerWrapper(urlAnalyzer, mapping);
		IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, perFieldAnalyzer);
		iwriter = new IndexWriter(directory, config);
//		if(!forceSameDir) iwriter.deleteAll();
		iwriter.commit();

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("EndPoint2Lucene..., starting: " + dateFormat.format(date));

		start = System.currentTimeMillis();
		processEndPoints();
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime + " TotalTriples: " + totalTriples);
		
		date = new Date();
		System.out.println("Dump2Lucene...Parallel, finalize at: " + dateFormat.format(date));
		if (iwriter != null) {
			iwriter.close();
		}
		if (directory != null) {
			directory.close();
		}
		
	}

	private static void processEndPoints() throws IOException {
		Set<String> endPoints = Files.lines(Paths.get("GoodEndPoints.txt")).collect(Collectors.toSet());
		endPoints.removeAll(WimuUtil.getAlreadyProcessed(Wimu.logFileName));
		endPoints.parallelStream().forEach(endPoint -> {
			try {
				totalTriples += extract(endPoint, 9999);
				System.out.println("SUCESS: " + endPoint);
			} catch (ClassNotFoundException | SQLException | IOException e) {
				mEndPointError.put(endPoint, e.getMessage());
				System.out.println("FAIL: " + endPoint + " ERROR: " + e.getMessage());
				//e.printStackTrace();
			}
		});
		generateFile(mEndPointError, "EndpointErrors.csv", true);
	}

	private static void testEndPoint(String[] args) throws ClassNotFoundException, SQLException, IOException {
		long start = System.currentTimeMillis();
		// String endPoint = "http://dbpedia.org/sparql";
		String endPoint = args[0];
		int limit = Integer.parseInt(args[1]);
		long totalTriples = extract(endPoint, limit);
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime + " TotalTriples: " + totalTriples);
	}

	private static long extract(String endPoint, int limit) throws ClassNotFoundException, SQLException, IOException {
		long totalTriples = 0;

		System.out.println("SERIAL OFFSET: Extracting datatypes from: " + endPoint);

		ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();

		// final long offsetSize = 9900;
		final long offsetSize = limit;
		long offset = 0;
		do {
			String sparqlQueryString = "SELECT * WHERE { ?s ?p ?o . FILTER(isliteral(?o)) } offset " + offset
					+ " limit " + offsetSize;
			// System.out.println(sparqlQueryString);

			//long start = System.currentTimeMillis();

			Query query = QueryFactory.create(sparqlQueryString);

			int cDtypes = 0;
			int countOffsetTriples = 0;
			String uriDataset = null;
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			try {
				ResultSet results = qexec.execSelect();
				for (; results.hasNext();) {
					try {
						QuerySolution soln = results.nextSolution();
						uriDataset = soln.get("?s").toString() + "\t" + endPoint;
						if (mDatatypeTriples.containsKey(uriDataset)) {
							cDtypes = mDatatypeTriples.get(uriDataset) + 1;
							mDatatypeTriples.put(uriDataset, cDtypes);
						} else {
							mDatatypeTriples.put(uriDataset, 1);
						}
						countOffsetTriples++;
						totalTriples++;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				break;
			} finally {
				qexec.close();
			}
			//long totalTime = System.currentTimeMillis() - start;
			//System.out.println("offset: " + offset + " endPoint:" + endPoint + " - Total time: " + totalTime
			//		+ " triples: " + countOffsetTriples + " MapSize: " + mDatatypeTriples.size());
			offset += offsetSize;
			if (mDatatypeTriples.size() > (Integer.MAX_VALUE - 3)) {
				System.out.println("MAX number of elements HashMap, inserting lucene.");
				insertLucene(mDatatypeTriples);
				mDatatypeTriples.clear();
				mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
			}
			// generateFile(mDatatypeTriples, "VeryHugeFile.nt");
			// System.exit(0);
		} while (true);
		
		if(mDatatypeTriples.mappingCount() > 0)
			insertLucene(mDatatypeTriples);
		
		return totalTriples;
	}

	private static void insertLucene(ConcurrentHashMap<String, Integer> mDatatypeTriples2) throws IOException {
		mDatatypeTriples2.entrySet().parallelStream().forEach(elem ->{
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

	public static void generateFile(Map<String, Integer> maps, String fileName) {
		File ret = new File(fileName);
		System.out.println("Generating file: " + ret.getAbsolutePath());
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			maps.forEach((uriDataset, dTypes) -> {
				writer.println(uriDataset + " \"" + dTypes + "\"^^<http://www.w3.org/2001/XMLSchema#int> .");
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("File generated: " + ret.getAbsolutePath());
	}

	public static File generateFile(Map<String, String> endPointErrors, String fileName, boolean b) {
		File ret = new File(fileName);
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
}
