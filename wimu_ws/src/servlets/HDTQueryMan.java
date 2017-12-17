package servlets;


import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

public class HDTQueryMan {

	static Map<String, String> md5Names = new HashMap<String, String>();
	public static void loadFileMap(String md5ConfigFile) throws IOException{
		File f = new File(System.getProperty("user.home") + "/" + md5ConfigFile);
		if(!f.exists()){
			System.err.println("md5ConfigFile not found.");
			return;
		}
		List<String> lstLines = FileUtils.readLines(f, "UTF-8");
		for (String line : lstLines) {
			String s[] = line.split("\t");
			if(s.length < 2) continue;
			String md5 = s[0];
			String urlDataset = s[1];
			md5Names.put(md5, urlDataset);
		}
	}
	
	/*
	 * returns the count of literals that a subject has.
	 */
	public static int getHDTDTypes(File dataset, String uri) {
		int ret = 0;
		HDT hdt;
		try {
			//hdt = HDTManager.loadIndexedHDT(fileHDT, null);
			hdt = HDTManager.mapHDT(dataset.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			//String query1 = "Select (count(?s) as ?c) where {?s ?p ?o . filter(?s=bnode("+uri+") && isLiteral(?o)) }";
			String query1 = "Select (count(?s) as ?c) where {?s ?p ?o . filter(?s=<"+uri+"> && isLiteral(?o)) }";
			//query1 = URLEncoder.encode(query1, "UTF-8");
			ret = sparql(model, query1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ret;
	}
	
	public static int sparql(Model model, String sparql) {
		int ret = 0;
		//StopWatch st = new StopWatch();
		
		Query query = QueryFactory.create(sparql);

		// Execute the query and obtain results
		QueryExecution qe = QueryExecutionFactory.create(query, model);
		ResultSet results = qe.execSelect();

		if(results.hasNext()){
	        QuerySolution thisRow = results.next();
	        Literal C_12_literal = ((Literal) thisRow.get("c"));
	        ret = C_12_literal.getInt();
	    }
		
		// Output query results	
		//ResultSetFormatter.out(System.out, results, query);

		// Important - free up resources used running the query
		qe.close();
		return ret;
	}

	public static Map<String, Integer> findDatasetsHDT(String uri) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		
		// All files from the directory hdtDatasets
		// if is to slow, I'll do a HDTDBindex only with datatypes.
		Set<File> datasets;
		try {	
			datasets = getFiles(new File(System.getProperty("user.home") + "/hdtDatasets"));
			for (File dataset : datasets) {
			//datasets.parallelStream().forEach(dataset -> {	
				int dTypes = getHDTDTypes(dataset, uri);
				if (dTypes < 1) continue;
//				String md5 = dataset.getName().substring(0, dataset.getName().indexOf("."));
//				String dsName = md5Names.get(md5);
//				dsName = (dsName != null) ? dsName : dataset.getName();
//				result.put(dsName, dTypes);
				result.put(dataset.getName(), dTypes);
			}
			//});	
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static Set<File> getFiles(File dir) throws IOException {
		Set<File> setFiles = null;
		if (dir.isDirectory()) {
			setFiles = Files.walk(Paths.get(dir.getPath())).filter(Files::isRegularFile).map(Path::toFile)
					.collect(Collectors.toSet());
		}
		return setFiles;
	}
	
	public static String printDatasets(){
		StringBuffer sb = new StringBuffer("");
//		String line = "<tr> <td><a href='xx' target='_blank' rel='noopener'>blbaba</a></td> "
//				+ "<td>888</td> <td>9999</td> <td>HDT</td> </tr>";
//		sb.append(line);
//		return sb.toString();
		
		Set<File> datasets;
		try {	
			datasets = getFiles(new File(System.getProperty("user.home") + "/hdtDatasets"));
			for (File dataset : datasets) {
				int totalTriples = getCountTriples(dataset);
				int totalDTypes = getCountDataTypes(dataset);
				String line = "<tr> <td><a href='xx' target='_blank' rel='noopener'>"+dataset.getName()+"</a></td> "
						+ "<td>"+totalTriples+"</td> <td>"+totalDTypes+"</td> <td>HDT</td> </tr>";
				sb.append(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	private static int getCountTriples(File dataset) {
		int ret = 0;
		HDT hdt;
		try {
			//hdt = HDTManager.loadIndexedHDT(fileHDT, null);
			hdt = HDTManager.mapHDT(dataset.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			String query1 = "Select (count(?s) as ?c) where {?s ?p ?o}";
			
			ret = sparql(model, query1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ret;
	}

	private static int getCountDataTypes(File dataset) {
		int ret = 0;
		HDT hdt;
		try {
			//hdt = HDTManager.loadIndexedHDT(fileHDT, null);
			hdt = HDTManager.mapHDT(dataset.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			String query1 = "Select (count(?s) as ?c) where {?s ?p ?o . filter(isLiteral(?o)) }";
			
			ret = sparql(model, query1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ret;
	}

	public static void main(String args[]) throws IOException{
		loadFileMap("md5HDT.csv");
		Map<String, Integer> mRes = findDatasetsHDT("http://dbpedia.org/resource/Leipzig");
		System.out.println(mRes);
	}
}
