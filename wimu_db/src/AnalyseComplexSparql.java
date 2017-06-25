import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class AnalyseComplexSparql {
	public static Map<String, String> mEndPointError = new HashMap<String, String>();
	static Set<String> setGood = new HashSet<String>();

	public static void main(String args[]) {
		Set<String> setEndPoints = QueryEndPoints.getGoodEndPoints();
		System.out.println("Good EndPoints from LODStats: " + setEndPoints.size());
		// Set<String> setEndPoints = new HashSet<String>();
		// setEndPoints.add("http://jisc.rkbexplorer.com/sparql/");
		// setEndPoints.add("http://dbpedia.org/sparql");
		// setEndPoints.add("http://lov.okfn.org/endpoint/lov");
		System.out.println("Starting analisys--- NON parallel");
		long start = System.currentTimeMillis();
		for (String endPoint : setEndPoints) {
			try {
				TimeOut.analyseSparqlComplex(endPoint, 30);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// setEndPoints.parallelStream().forEach(endPoint -> {
		// //analyseSparqlComplex(endPoint);
		// try {
		// TimeOut.analyseSparqlComplex(endPoint, 30);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (ExecutionException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// });
		long totalTime = System.currentTimeMillis() - start;
		generateFile(mEndPointError, "EndPointErrorsTest.csv");
		generateFile(setGood, "EndPointGoodTest.csv");
		System.out.println("Total time: " + totalTime);
		System.out.println("Errors: " + mEndPointError.size());
		System.out.println(mEndPointError);
		System.out.println("Good:" + setGood.size());
		System.out.println(setGood);
	}

	private static void generateFile(Set<String> setGood2, String fileName) {
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			setGood2.forEach(endPoint -> {
				writer.println(endPoint + "\n");
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void analyseSparqlComplex(String endPoint) {

		String sparqlQueryString = "select ?x (count(?z) as ?c) { ?x ?y ?z . \n "
				+ "filter (datatype(?z) != '' || lang(?z) != '') } \n" + "group by ?x  \n";

		Query query = QueryFactory.create(sparqlQueryString);

		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);

		try {
			ResultSet results = qexec.execSelect();
			for (; results.hasNext();) {
				try {
					QuerySolution soln = results.nextSolution();
					String subj = soln.get("?x").toString();
					setGood.add(endPoint);
					System.out.println("Good: " + endPoint);
					break;
				} catch (Exception e) {
				}
				// result.add(soln.get("?strLabel").toString());
				// System.out.println(soln.get("?strLabel"));
			}
		} catch (Exception e) {
			mEndPointError.put(endPoint, e.getMessage());
			System.err.println("Bad: " + endPoint);
		}

		finally {
			qexec.close();
		}

	}

	public static ResultSet isGood(String endPoint) {
		ResultSet result = null;

		String sparqlQueryString = "select ?x (count(?z) as ?c){?x ?y ?z . \n "
				+ "filter (isliteral(?z))} group by ?x";
		//String sparqlQueryString = "select ?x (count(?z) as ?c) { ?x ?y ?z . \n "
		//		+ "filter (datatype(?z) != '' || lang(?z) != '') } \n" + "group by ?x  \n";

		Query query = QueryFactory.create(sparqlQueryString);

		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);

		try {
			result = qexec.execSelect();
		} catch (Exception e) {
			mEndPointError.put(endPoint, e.getMessage());
			System.err.println("Bad: " + endPoint);
		} finally {
			qexec.close();
		}
		return result;
	}

	/*
	 * Return a set of good endPoints.
	 */
	public static Set<String> createDBIndex(Set<String> endpointsGoodHTML) {
		Set<String> result = new HashSet<String>();
		// for (String endPoint : endpointsGoodHTML) {
		endpointsGoodHTML.parallelStream().forEach(endPoint -> {
			try {
				// TimeOut.analyseSparqlComplex(endPoint, 30);
				ResultSet results = TimeOut.isGood(endPoint, 300);
				if (results != null) {
					DBUtil.setAutoCommit(false);
					for (; results.hasNext();) {
						try {
							result.add(endPoint);
							QuerySolution soln = results.nextSolution();
							String subj = soln.get("?x").toString();
							int cDType = soln.getLiteral("?c").getInt();
							// countDType = countDType.replaceAll("\\D+","");
							// int cDType = Integer.parseInt(countDType);
							DBUtil.insert(endPoint, subj, cDType);
						} catch (Exception e) {
						}
					}
					DBUtil.setAutoCommit(true);
				} else {
					mEndPointError.put(endPoint, "TimeOut.");
					System.err.println("TimeOut: " + endPoint);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//continue;
				return;
			}
		});

		return result;
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
}