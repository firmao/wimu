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
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
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
	public static final Version LUCENE_VERSION = Version.LUCENE_44;
	private static Analyzer urlAnalyzer;
	private static IndexWriter iwriter;
	private static MMapDirectory directory;

	public static long start = 0;
	public static long start2 = 0;
	public static long startEndpoint = 0;
	public static long totalTime = 0;
	public static long countDir = 0;
	public static final long lim = 100000000;
	public static final long oneDay = 86400000;
    public static final long oneHour = 3600000;

	public static void main(String[] args) throws Exception {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		start = System.currentTimeMillis();
		//processEndPoints();
		long totalTriples = extractParallel("http://live.dbpedia.org/sparql");
		//long totalTriples = extract("http://live.dbpedia.org/sparql");
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime + " TotalTriples: " + totalTriples);
	}

	public static void create(String pLuceneDir) throws IOException {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("EndPoint2Lucene..., starting: " + dateFormat.format(date));

		start = System.currentTimeMillis();
		start2 = System.currentTimeMillis();
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
		++countDir;
		File indexDirectory = new File("luceneDir_" + countDir);
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
		iwriter.commit();
		
		Set<String> endPoints = Files.lines(Paths.get("GoodEndPoints.txt")).collect(Collectors.toSet());
		endPoints.removeAll(WimuUtil.getAlreadyProcessed(Wimu.logFileName));
		endPoints.parallelStream().forEach(endPoint -> {
		//endPoints.forEach(endPoint -> {
			try {
				totalTriples += extract(endPoint, 9999);
				//totalTriples += extractParallel(endPoint);
				System.out.println("SUCESS: " + endPoint);
			} catch (Exception e) {
				System.out.println("FAIL: " + endPoint + " ERROR: " + e.getMessage());
				// e.printStackTrace();
			}
		});
		generateFile(mEndPointError, "EndpointErrors.csv", true);
	}

	private static void testEndPoint(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		// String endPoint = "http://dbpedia.org/sparql";
		String endPoint = args[0];
		
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime + " TotalTriples: " + totalTriples);
	}

	private static synchronized long extractParallel(String endPoint) throws Exception {
		//long countOffset = 0;
		final long offsetSize = 10000;
		//long offset = 0;
		startEndpoint = System.currentTimeMillis();
		
		Set<Long> setOffsets = getSetOffsets(offsetSize, 100);
		System.out.println("Stating to process: " + endPoint);
		setOffsets.parallelStream().forEach(offset -> {
			totalTime = System.currentTimeMillis() - startEndpoint;
			String sparqlQueryString = "SELECT ?s (count(?o) as ?c) WHERE { ?s ?p ?o . FILTER(isliteral(?o)) } group by ?s offset " + offset
					+ " limit " + offsetSize;

			Query query = QueryFactory.create(sparqlQueryString);

			int dType = 0;
			String uri = null;
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			try {
				// qexec.setTimeout(300000); 5 minutes timeout
				ResultSet results = qexec.execSelect();
				if(!results.hasNext()){
					System.out.println("Stoped on offset: " + offset + ", there is no more data from: " + endPoint);
					return;
				}
				for (; results.hasNext();) {
					try {
						QuerySolution soln = results.nextSolution();
						dType = soln.get("?c").asLiteral().getInt();
						uri = soln.get("?s").toString();
						try {
							Document doc = new Document();
							doc.add(new StringField("uri", uri, Store.YES));
							doc.add(new StringField("dataset_dtype", endPoint + "\t" + dType, Store.YES));
							iwriter.addDocument(doc);
						} catch (Exception ex) {
							//System.out.println("Endpoint1: "+endPoint+" Error: " + ex.getMessage());
						}
						totalTriples++;
					} catch (Exception e) {
						//System.out.println("Endpoint2: "+endPoint+" Error: " + e.getMessage());
						throw e;
					}
				}
			} catch (Exception en) {
				//mEndPointError.put(endPoint, en.getMessage());
				//System.out.println("Endpoint3: "+endPoint+" Error: " + en.getMessage());
				throw en;
			} finally {
				qexec.close();
			}
			
			//totalTime = System.currentTimeMillis() - start2;
			//if((totalTriples > 1073741820) || (totalTime > oneDay)){
			try {
				checkLimit();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

		if (iwriter != null) {
			iwriter.close();
		}
		if (directory != null) {
			directory.close();
		}
		
		return totalTriples;
	}
	
	private static Set<Long> getSetOffsets(long offsetSize, int times) {
		Set<Long> ret = new HashSet<Long>();
		//long size = offsetSize * times;
		long offset = 0;
		for (int i = 0; i < times; i++) {
			ret.add(offset); 
			offset += offsetSize;
		}
		return ret;
	}

	private static synchronized long extract(String endPoint) throws Exception {
		long countOffset = 0;
		final long offsetSize = 10000;
		long offset = 0;
		startEndpoint = System.currentTimeMillis();
		
		System.out.println("Stating to process: " + endPoint);
		do {
			totalTime = System.currentTimeMillis() - startEndpoint;
			if(totalTime > oneHour){
				throw new Exception("Timeout, more than 1 hour.");
			}
//			System.out.println("offset: " + countOffset);
//			++countOffset;
			String sparqlQueryString = "SELECT ?s (count(?o) as ?c) WHERE { ?s ?p ?o . FILTER(isliteral(?o)) } group by ?s offset " + offset
					+ " limit " + offsetSize;

			Query query = QueryFactory.create(sparqlQueryString);

			int dType = 0;
			String uri = null;
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			try {
				// qexec.setTimeout(300000); 5 minutes timeout
				ResultSet results = qexec.execSelect();
				if(!results.hasNext()){
					System.out.println("Stoped on offset: " + offset + ", there is no more data from: " + endPoint);
					break;
				}
				for (; results.hasNext();) {
					try {
						QuerySolution soln = results.nextSolution();
						dType = soln.get("?c").asLiteral().getInt();
						uri = soln.get("?s").toString();
						try {
							Document doc = new Document();
							doc.add(new StringField("uri", uri, Store.YES));
							doc.add(new StringField("dataset_dtype", endPoint + "\t" + dType, Store.YES));
							iwriter.addDocument(doc);
						} catch (Exception ex) {
							//System.out.println("Endpoint1: "+endPoint+" Error: " + ex.getMessage());
						}
						totalTriples++;
					} catch (Exception e) {
						//System.out.println("Endpoint2: "+endPoint+" Error: " + e.getMessage());
						throw e;
					}
				}
			} catch (Exception en) {
				//mEndPointError.put(endPoint, en.getMessage());
				//System.out.println("Endpoint3: "+endPoint+" Error: " + en.getMessage());
				throw en;
			} finally {
				qexec.close();
			}
			
			//totalTime = System.currentTimeMillis() - start2;
			//if((totalTriples > 1073741820) || (totalTime > oneDay)){
			checkLimit();
			offset += offsetSize;
		} while (true);

		if (iwriter != null) {
			iwriter.close();
		}
		if (directory != null) {
			directory.close();
		}
		
		return totalTriples;
	}

	private static synchronized void checkLimit() throws IOException {
		if(totalTriples > 1073741820){
			if (iwriter != null) {
				iwriter.close();
			}
			if (directory != null) {
				directory.close();
			}
			
			++countDir;
			File indexDirectory = new File("luceneDir_" + countDir);
			indexDirectory.mkdir();
			directory = new MMapDirectory(indexDirectory);
			System.out.println("Created Lucene dir: " + indexDirectory.getAbsolutePath() + "TotalTriples: " + totalTriples + " TotalTime: " + totalTime);
			urlAnalyzer = new SimpleAnalyzer(LUCENE_VERSION);
			Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
			mapping.put("uri", urlAnalyzer);
			mapping.put("dataset_dtype", urlAnalyzer);
			PerFieldAnalyzerWrapper perFieldAnalyzer = new PerFieldAnalyzerWrapper(urlAnalyzer, mapping);
			IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, perFieldAnalyzer);
			iwriter = new IndexWriter(directory, config);
			iwriter.commit();
			start2 =System.currentTimeMillis();
		}
	}

	private static synchronized long extract(String endPoint, int limit) throws Exception {
		long totalTriples = 0;
		String errorMessage = null;

		System.out.println("SERIAL OFFSET: Extracting datatypes from: " + endPoint);

		ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();

		// final long offsetSize = 9900;
		final long offsetSize = limit;
		long offset = 0;
		do {
			String sparqlQueryString = "SELECT * WHERE { ?s ?p ?o . FILTER(isliteral(?o)) } offset " + offset
					+ " limit " + offsetSize;
			// System.out.println(sparqlQueryString);

			// long start = System.currentTimeMillis();

			Query query = QueryFactory.create(sparqlQueryString);

			int cDtypes = 0;
			int countOffsetTriples = 0;
			String uriDataset = null;
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			try {
				// qexec.setTimeout(300000); 5 minutes timeout
				ResultSet results = qexec.execSelect();
				if(!results.hasNext()){
					System.out.println("Stoped on offset: " + offset + ", there is no more data from: " + endPoint);
					break;
				}
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
				mEndPointError.put(endPoint, e.getMessage());
				errorMessage = e.getMessage();
				break;
			} finally {
				qexec.close();
			}
			// long totalTime = System.currentTimeMillis() - start;
			// System.out.println("offset: " + offset + " endPoint:" + endPoint
			// + " - Total time: " + totalTime
			// + " triples: " + countOffsetTriples + " MapSize: " +
			// mDatatypeTriples.size());
			System.out.println("offset: " + offset);
			offset += offsetSize;
			// if (mDatatypeTriples.size() > (Integer.MAX_VALUE - 3)) {
			if (mDatatypeTriples.size() > lim) {
				System.out.println("MAX number of elements HashMap, inserting lucene.");
				insertLucene(mDatatypeTriples);
				mDatatypeTriples.clear();
				mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
			}
			// generateFile(mDatatypeTriples, "VeryHugeFile.nt");
			// System.exit(0);
		} while (true);

		if (mDatatypeTriples.mappingCount() > 0)
			insertLucene(mDatatypeTriples);

		if (errorMessage != null)
			throw new Exception(errorMessage);

		return totalTriples;
	}

	private static void insertLucene(ConcurrentHashMap<String, Integer> mDatatypeTriples2) throws IOException {
		System.out.println("Inserting Lucene " + mDatatypeTriples2.size() + " dataType triples");
		mDatatypeTriples2.entrySet().parallelStream().forEach(elem -> {
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
