import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
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

public class EndPoint2NT {

	private static long totalTriples=0;
	static ConcurrentHashMap<String, String> mEndPointError = new ConcurrentHashMap<String, String>();
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		long start = System.currentTimeMillis();
		processEndPoints();
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime
				+ " TotalTriples: " + totalTriples);
	}
	
	private static void processEndPoints() throws IOException{
		Set<String> endPoints = Files.lines(Paths.get("GoodEndPoints.txt")).collect(Collectors.toSet());
		endPoints.parallelStream().forEach(endPoint -> {
			try {
				totalTriples += extract(endPoint, 9999);
			} catch (ClassNotFoundException | SQLException e) {
				mEndPointError.put(endPoint, e.getMessage());
				e.printStackTrace();
			}
		});
		generateFile(mEndPointError, "EndpointErrors.csv", true);
	}
	
	private static void testEndPoint(String[] args) throws ClassNotFoundException, SQLException{
		long start = System.currentTimeMillis();
		//String endPoint = "http://dbpedia.org/sparql";
		String endPoint = args[0];
		int limit = Integer.parseInt(args[1]);
		long totalTriples = extract(endPoint, limit);
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime
				+ " TotalTriples: " + totalTriples);
	}
	
	private static long extract(String endPoint, int limit) throws ClassNotFoundException, SQLException {
		long totalTriples = 0;
		
		System.out.println("SERIAL OFFSET: Extracting datatypes from: " + endPoint);
				
		Map<String, Integer> mDatatypeTriples = new HashMap<String, Integer>();

		//final long offsetSize = 9900;
		final long offsetSize = limit;
		long offset = 0;
		do{
			String sparqlQueryString = "SELECT * WHERE { ?s ?p ?o . FILTER(isliteral(?o)) } offset "
					+ offset + " limit " + offsetSize;
			// System.out.println(sparqlQueryString);

			long start = System.currentTimeMillis();

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
						uriDataset = "<"+soln.get("?s").toString() + "> <" + endPoint + "> ";
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
			long totalTime = System.currentTimeMillis() - start;
			System.out.println("offset: " + offset + " endPoint:" + endPoint + " - Total time: " + totalTime
					+ " triples: " + countOffsetTriples + " MapSize: " + mDatatypeTriples.size());
			offset += offsetSize;
			if(mDatatypeTriples.size() > (Integer.MAX_VALUE-3)){
				System.out.println("MAX number of elements HashMap");
				String fileName = endPoint.replaceAll("/", ";");
				generateFile(mDatatypeTriples, fileName + "_" + totalTriples + ".nt");
				mDatatypeTriples.clear();
				mDatatypeTriples = new HashMap<String, Integer>();
			}
//			generateFile(mDatatypeTriples, "VeryHugeFile.nt");
//			System.exit(0);
		}while(true);
		String fileName = endPoint.replaceAll("/", ";");
		generateFile(mDatatypeTriples, fileName + ".nt");
		return totalTriples;
	}

	public static void generateFile(Map<String, Integer> maps, String fileName) {
		File ret = new File(fileName);
		System.out.println("Generating file: " + ret.getAbsolutePath());
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			maps.forEach((uriDataset, dTypes) -> {
				writer.println(uriDataset + " \""+dTypes+"\"^^<http://www.w3.org/2001/XMLSchema#int> .");
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
