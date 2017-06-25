import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class FirstOptimization {

 	static Map<String, String> mEndPointError = new HashMap<String, String>();
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		
		long startTotal = System.currentTimeMillis();
		System.out.println("Starting get good EndPointsHTML");
		long start = System.currentTimeMillis();
		Set<String> endPointsGoodHTML = QueryEndPoints.getGoodEndPoints();
		//Set<String> endPointsGoodHTML = Files.lines(Paths.get("GoodEndPoints_3.txt")).collect(Collectors.toSet());
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time (endPointsGoodHTML): " + totalTime);
		System.out.println("Number of endPointsGoodHTML: " + endPointsGoodHTML.size());
		generateFile(endPointsGoodHTML, "goodHTML.txt");
		
		System.out.println("Starting endpoints sparql count");
		start = System.currentTimeMillis();
		Set<String> endPointsSparql = AnalyseComplexSparql.createDBIndex(endPointsGoodHTML);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time (endPointsSparql): " + totalTime);
		System.out.println("Number of endPointsSparql: " + endPointsSparql.size());
		generateFile(endPointsSparql, "spaqlEndPoints.txt");
		
		start = System.currentTimeMillis();
		System.out.println("Trying recover possible redirects...Sparql complex count");
		Set<String> endPointsRedirectSparql = new HashSet<String>();
		AnalyseComplexSparql.mEndPointError.forEach((endPoint, error) ->{
			try {
				String redirectSparql = QueryEndPoints.getRedirection(endPoint);
				endPointsRedirectSparql.add(redirectSparql);
			} catch (IOException e) {
			}
		});
		Set<String> endPointsSparqlRedirect = AnalyseComplexSparql.createDBIndex(endPointsRedirectSparql);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time (Recover Redirect endPointsSparql): " + totalTime);
		System.out.println("Number of endPointsSparqlRedirect: " + endPointsSparqlRedirect.size());
		generateFile(endPointsSparqlRedirect, "spaqlEndPointsRedirect.txt");
		
		System.out.println("Starting endPoints BruteForce.");
		start = System.currentTimeMillis();
		Set<String> endPointsBruteForce = new HashSet<String>();
		endPointsBruteForce.addAll(endPointsGoodHTML);
		endPointsBruteForce.removeAll(endPointsSparql);
		endPointsBruteForce.removeAll(endPointsSparqlRedirect);
		Map<String, String> endPointsFailBruteForce = createDBIndex(endPointsBruteForce);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time (endPointsFailBruteForce): " + totalTime);
		System.out.println("Number of endPointsBruteForce: " + endPointsBruteForce.size());
		System.out.println("Number of endPointsFailBruteForce: " + endPointsFailBruteForce.size());		
		generateFile(endPointsBruteForce, "BruteForceEndPoints.txt");
		
		System.out.println("Trying recover possible redirects...BruteForce");
		start = System.currentTimeMillis();
		Set<String> endPointsRedirect = new HashSet<String>();
		endPointsFailBruteForce.forEach((endPoint, error) ->{
			try {
				String redirect = QueryEndPoints.getRedirection(endPoint);
				endPointsRedirect.add(redirect);
			} catch (IOException e) {
			}
		});
		Map<String, String> endPointsFailRedirect = createDBIndex(endPointsRedirect);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time (Redirects Brute Force): " + totalTime);
		
		long totalTimeAll = System.currentTimeMillis() - startTotal;
		System.out.println("Total time (EVERYTHING): " + totalTimeAll);
		System.out.println("Now writing the files...");
		
		generateFile(endPointsFailBruteForce, "FailBruteForceEndPoints.csv");
		generateFile(endPointsFailRedirect, "FailRedirectEndPoints.csv");
	}

	private static Map<String, String> createDBIndex(Set<String> setEndPoints) throws ClassNotFoundException, SQLException {
		
		/*
		 * For each triple in dump file or Endpoint: If object typeof datatype:
		 * Count +1 for subject URI
		 */
		// if((args.length > 0) && (args[0].equals("false"))) DEBUG=false;
		diagnoses();
		Map<String, String> mSubjEndPoint = new HashMap<String, String>();
		Map<String, Integer> mSubjCount = new HashMap<String, Integer>();
		System.out.println("Number of endPoints: " + setEndPoints.size());
		long start = System.currentTimeMillis();
		System.out.println("Parallel version:");
		try {
			setEndPoints.parallelStream().forEach(endPoint -> {
				// setEndPoints.forEach(endPoint -> {
				Set<String> setTriples = getTriples(endPoint);
				Set<String> dTypes = new HashSet<String>();
				// setTriples.parallelStream().forEach(triple -> { // Parallel concurrency problems.
				setTriples.forEach(triple -> {
					String[] sTriple = triple.split("\t", -1);
					String subj = sTriple[0];
					String pred = sTriple[1];
					String obj = sTriple[2];
					//System.out.println("Insert Subjects and counts into a relational DB.");
					try{
						DBUtil.insert(subj, endPoint);
					}catch(Exception e){
						System.err.println(e.getMessage());
					}
					dTypes.add(obj);
					mSubjEndPoint.put(subj, endPoint);
					if (mSubjCount.containsKey(subj)) {
						Integer value = mSubjCount.get(subj);
						mSubjCount.put(subj, value + 1);
					} else {
						mSubjCount.put(subj, 1);
					}
					
				});
				System.out.println("EndPoint: " + endPoint + "\nTotalNumberOfTriples: " + setTriples.size()
							+ "\nNumDataTypes: " + dTypes.size());
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total time: " + totalTime
				+ " -- Depends on the internet connexion, because need to access the endpoints.");
		System.out.println("Number of Subjects with Objects as DataType: " + mSubjCount.size());
		System.out.println("Number of DataTypes: " + getTotalDType(mSubjCount));
		System.out.println(
				"Number of selected Datasets(endpoints with dataTypes): " + getSelectedEndPoints(mSubjEndPoint));
		System.out.println("Number of endPoints (Still some error): " + mEndPointError.size());
		generateFile(mEndPointError, "EndPointErrors.csv");
//		Email.sendEmail("firmao@gmail.com", "LinkLion part1 finished", "Total time: " + totalTime + "\n"
//				+ "Number of Subjects with Objects as DataType: " + mSubjCount.size() + "\n"
//				+ "Number of selected Datasets(endpoints with dataTypes): " + getSelectedEndPoints(mSubjEndPoint) + "\n"
//				+ "Number of endPoints (Still some error): " + mEndPointError.size() + "\n"
//				+ "Number of endPoints: " + setEndPoints.size());
		return mEndPointError;
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

	public static File generateFile(Set<String> endPointErrors, String fileName) {
		File ret = new File(fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			endPointErrors.forEach(endPoint -> {
				writer.println(endPoint);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}
	
	private static int getSelectedEndPoints(Map<String, String> mSubjEndPoint) {
		Set<String> setTest = new HashSet<String>();
		setTest.addAll(mSubjEndPoint.values());
		return setTest.size();
	}
	
	private static Integer getTotalDType(Map<String, Integer> mSubjCount) {
		// mSubjCount.entrySet().parallelStream().forEach(action);
		Integer iSum = 0;
		for (Integer elem : mSubjCount.values()) {
			iSum += elem;
		}
		return iSum;
	}
	
	private static void diagnoses() throws ClassNotFoundException, SQLException {
		Connection cnn = DBUtil.getConnection();
		if(cnn != null) 
			System.out.println("DB connection OK");
		else
			System.err.println("ALERT !!!! No DB connection!");
	}
	
	public static Set<String> getTriples(String endPoint) {
		Set<String> setReturn = new HashSet<String>();
		String sparqlQueryString = "SELECT * WHERE { ?s ?p ?o }";

		Query query = QueryFactory.create(sparqlQueryString);

		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);

		try {
			ResultSet results = qexec.execSelect();
			for (; results.hasNext();) {
				try {
					QuerySolution soln = results.nextSolution();
					if (!soln.get("?o").isLiteral())
						continue;
					String subj = soln.get("?s").toString();
					String pred = soln.get("?p").toString();
					String obj = soln.get("?o").toString();

					String line = subj + "\t" + pred + "\t" + obj;
					setReturn.add(line);
				} catch (Exception e) {
				}
				// result.add(soln.get("?strLabel").toString());
				// System.out.println(soln.get("?strLabel"));
			}
		} catch (Exception e) {
			mEndPointError.put(endPoint, e.getMessage());
			return setReturn;
		}

		finally {
			qexec.close();
		}

		return setReturn;
	}
}
