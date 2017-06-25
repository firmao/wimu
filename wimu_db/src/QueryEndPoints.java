import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class QueryEndPoints {

	public static void main(String[] args) throws MalformedURLException, IOException {
//		System.out.println("Starting to get EndPoints(URLs) from LODstats");
//		Set<String> urls = getURLS();
//		System.out.println("Starting analysing endpoints...");
//		Set<String> goodURLs = new HashSet<String>();
//
//		long start = System.currentTimeMillis();
//		urls.parallelStream().forEach(url -> {
//		//for (String url : urls) {
//			try {
//				HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
//				connection.setRequestMethod("HEAD");
//				int responseCode = connection.getResponseCode();
//				if (responseCode == 200) {
//					//if(canQueryEndPoint(url))
//					String urlReal = getRedirection(responseCode, url, connection);
//					goodURLs.add(urlReal);
//				}
//
//				// < 100 is undetermined.
//				// 1nn is informal (shouldn't happen on a GET/HEAD)
//				// 2nn is success
//				// 3nn is redirect
//				// 4nn is client error
//				// 5nn is server error
//			} catch (Exception ex) {
//				//ex.printStackTrace();
//			}
//		});
//		//}	
//		long totalTime = System.currentTimeMillis() - start;
//		System.out.println("Total time (Get Good endPoints): " + totalTime);
//		System.out.println("Generating File with good endpoints...");
//		generateFile(goodURLs, "GoodEndPoints.txt");
//		System.out.println("End");
		
		Set<String> setGoodEndpoints = getGoodEndPoints();
		generateFile(setGoodEndpoints, "GoodEndPoints_1.txt");
		
		long start = System.currentTimeMillis();
		Map<String, String> mRedGoodEndpoints = new HashMap<String, String>();
		for (String endPoint : setGoodEndpoints) {
			String urlRed = getRedirection(endPoint);
			if(!urlRed.equals(endPoint))
				mRedGoodEndpoints.put(endPoint, urlRed);
		}
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time (Redirects): " + totalTime);
		generateFile(mRedGoodEndpoints, "GoodEndPoints_Redirect.txt");
		
		
//		String url = "http://beta.sparql.uniprot.org/";
//		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
//		String redirection = getRedirection(conn);
//		System.out.println(redirection);
	}
	
	public static File generateFile(Map<String, String> endPointErrors, String fileName) {
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

	public static String getRedirection(String url) throws MalformedURLException, IOException {
		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		String urlReal = null;
		urlReal = conn.getHeaderField("Location");
		urlReal = conn.toString().split(":")[1] + ":" + conn.toString().split(":")[2];
		return urlReal;
	}

	public static Set<String> getGoodEndPoints() {
		System.out.println("Starting to get EndPoints from LODstats");
		Set<String> urls = getURLS();
		System.out.println("Starting analysing "+ urls.size() +" endpoints from LODStats (Searching for only good endPoints)...");
		Set<String> goodURLs = new HashSet<String>();

		long start = System.currentTimeMillis();
		urls.parallelStream().forEach(url -> {
		//for (String url : urls) {
			try {
				HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
				connection.setRequestMethod("HEAD");
				int responseCode = connection.getResponseCode();
				if (responseCode == 200) {
					//String urlRedirect = getRedirection(connection);
					//goodURLs.add(urlReal);
					goodURLs.add(url);
					//mEndPointsRedirects.put(url,urlRedirect);
				}

				// < 100 is undetermined.
				// 1nn is informal (shouldn't happen on a GET/HEAD)
				// 2nn is success
				// 3nn is redirect
				// 4nn is client error
				// 5nn is server error
			} catch (Exception ex) {
				//ex.printStackTrace();
			}
		});
		//}	
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time (Get Good endPoints): " + totalTime);
		return goodURLs;
	}

	private static boolean canQueryEndPoint(String url) {
		String sparqlQueryString = "ASK{}";

		Query query = QueryFactory.create(sparqlQueryString);

		QueryExecution qexec = QueryExecutionFactory.sparqlService(url, query);
		//QueryEngineHTTP qexec = (QueryEngineHTTP) QueryExecutionFactory.sparqlService(url, query);
		try {
			return qexec.execAsk();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		finally {
			qexec.close();
		}
	}

	private static Set<String> getURLS() {
		Set<String> result = new HashSet<String>();

		String sparqlQueryString = "prefix xsd:  <http://www.w3.org/2001/XMLSchema#> \n "
				+ "SELECT DISTINCT ?url where { \n"
				+ "?id a <http://lodstats.aksw.org/ontology/ldso.owl#Dataset> ; \n"
				+ "<http://purl.org/dc/terms/format> \"sparql\"^^xsd:string ; \n"
				+ "<http://www.w3.org/ns/dcat#downloadURL> ?url \n"
				+ "} ORDER BY ?url";

		Query query = QueryFactory.create(sparqlQueryString);

		QueryExecution qexec = QueryExecutionFactory.sparqlService("http://stats.lod2.eu/sparql", query);

		try {
			ResultSet results = qexec.execSelect();
			for (; results.hasNext();) {
				QuerySolution soln = results.nextSolution();
				String url = soln.get("?url").toString();
				result.add(url);
				//result.add(soln.get("?strLabel").toString());
				// System.out.println(soln.get("?strLabel"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		finally {
			qexec.close();
		}
		//generateFile(result, "AllEndPoints.txt");
		return result;
	}

	public static File generateFile(Set<String> endpoint, String fileName) {
		File ret = new File(fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			for (String line : endpoint) {
				writer.println(line);
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}
	
	public static void readFile(File file) throws IOException{
		
		List<String> lstLines = Files.lines(Paths.get("text.txt")).collect(Collectors.toList());
		
	}

	public static Set<String> getLogEndpoints(File fLog) throws IOException {
		Set<String> sRet = new HashSet<String>();
		Set<String> setLines = Files.lines(Paths.get(fLog.getName())).collect(Collectors.toSet());
		for (String line : setLines) {
			String[] sLine = line.split("EndPoint: ");
			if(sLine.length > 1)
				sRet.add(sLine[1].trim());
		}
		return sRet;
	}

	public static int getLogEndpoints(File fLog, String field) throws IOException {
		int ret = 0;
		Set<String> setLines = Files.lines(Paths.get(fLog.getName())).collect(Collectors.toSet());
		for (String line : setLines) {
			String[] sLine = line.split(field);
			if(sLine.length > 1){
				int value = Integer.parseInt(sLine[1].trim());
				ret += value;
			}
		}
		return ret;
	}
}
