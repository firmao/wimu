package servlets;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.tomcat.util.http.fileupload.FileUpload;

public class EndPoint {

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		int dTypes = getDataTypes("http://dbpedia.org/resource/Leipzig");
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Datatypes: "+dTypes+"TotalTime: " + totalTime);
		FileUpload f = new FileUpload();
	}

	public static int getDataTypes(String uri) {
		int ret = 0;
		//String sparqlQueryString = "Select (count(*) as ?c) WHERE { ?s ?p ?o . FILTER(isliteral(?o) && ?s=<http://dbpedia.org/resource/Leipzig>) }";
		String sparqlQueryString = "Select (count(?s) as ?c) where {?s ?p ?o . Filter(isLiteral(?o) && ?s=<"+uri+">)}";
		Query query = QueryFactory.create(sparqlQueryString);
		String endPoint = "http://dbpedia.org/sparql";
		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query, "http://dbpedia.org");
		try {
			ResultSet results = qexec.execSelect();
			for (; results.hasNext();) {
				try {
					QuerySolution soln = results.nextSolution();
					Literal count = soln.getLiteral("?c");
					ret = count.getInt();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.out.println(sparqlQueryString);
			e.printStackTrace();
		}
		finally {
			qexec.close();
		}
		return ret;
	}

}
