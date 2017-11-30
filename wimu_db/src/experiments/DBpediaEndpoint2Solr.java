import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

public class DBpediaEndpoint2Solr {

	private static long totalTriples = 0;
	static ConcurrentHashMap<String, String> mEndPointError = new ConcurrentHashMap<String, String>();

	public static long start = 0;
	public static long totalTime = 0;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		start = System.currentTimeMillis();
		HttpSolrClient solr = SolrUtil.initialize();
		processEndPoints(solr);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime + " TotalTriples: " + totalTriples);
	}

	public static void create() throws IOException, SolrServerException {
		HttpSolrClient solr = SolrUtil.initialize();
//		solr.deleteByQuery("*:*");
//		solr.commit();

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("DBpediaEndPoint2Solr..., starting: " + dateFormat.format(date));

		start = System.currentTimeMillis();
		processEndPoints(solr);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime + " TotalTriples: " + totalTriples);

		date = new Date();
		System.out.println("DBpediaEndpoint2Solr...Parallel, finalize at: " + dateFormat.format(date));

	}

	private static void processEndPoints(HttpSolrClient solr) throws IOException {
		String endPoint = "http://dbpedia.org/sparql";
//		Set<String> endPoints = Files.lines(Paths.get("GoodEndPoints.txt")).collect(Collectors.toSet());
//		endPoints.parallelStream().forEach(endPoint -> {
			try {
				totalTriples += extract(endPoint, 9999, solr);
			} catch (ClassNotFoundException | SQLException | IOException | SolrServerException e) {
				mEndPointError.put(endPoint, e.getMessage());
				e.printStackTrace();
			}
//		});
		generateFile(mEndPointError, "EndpointErrors.csv", true);
	}


	private static long extract(String endPoint, int limit, HttpSolrClient solr)
			throws ClassNotFoundException, SQLException, IOException, SolrServerException {
		long totalTriples = 0;

		System.out.println("PARALLEL ENDPOINTS: Extracting datatypes from: " + endPoint);

		ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();

		// final long offsetSize = 9900;
		final long offsetSize = limit;
		long offset = 0;
		do {
			String sparqlQueryString = "SELECT * WHERE { ?s ?p ?o . FILTER(isliteral(?o)) } offset " + offset
					+ " limit " + offsetSize;
			// System.out.println(sparqlQueryString);

			//long oStart = System.currentTimeMillis();

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
//			 long oTotalTime = System.currentTimeMillis() - oStart;
//			 System.out.println("offset: " + offset + " endPoint:" + endPoint
//			 + " - Total time: " + oTotalTime
//			 + " countOffsetTriples: " + countOffsetTriples 
//			 + " MapSize: " + mDatatypeTriples.size()
//			 + " totalTriples until the moment: " + totalTriples);
			offset += offsetSize;
			if (mDatatypeTriples.size() > (Integer.MAX_VALUE - 3)) {
				System.out.println("MAX number of elements HashMap");
				// insertLucene(mDatatypeTriples);
				SolrUtil.createIndex(mDatatypeTriples, solr);
				mDatatypeTriples.clear();
				mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
			}
			// generateFile(mDatatypeTriples, "VeryHugeFile.nt");
			// System.exit(0);
		} while (true);

		if (mDatatypeTriples.mappingCount() > 0)
			SolrUtil.createIndex(mDatatypeTriples, solr);

		return totalTriples;
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
