package servlets;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.queryparser.classic.ParseException;

import com.google.gson.Gson;

/**
 * Servlet implementation class Find
 */
public class Find extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static long timeLoadMD5HDT = 0;
	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Find() {
		super();
	}
	
	@Override
	public void init() throws ServletException {
		// TODO Auto-generated method stub
		super.init();
		try {
			long start = System.currentTimeMillis();
			HDTQueryMan.loadFileMap("md5HDT.csv");
			timeLoadMD5HDT = System.currentTimeMillis() - start;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			handleRequestAndRespond(request, response);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			handleRequestAndRespond(request, response);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void handleRequestAndRespond(HttpServletRequest request, HttpServletResponse response) throws ParseException {
		try {
			if (request.getParameter("uri") != null) {
				String uri = request.getParameter("uri");
				uri = uri.replaceAll("123nada", "#").trim(); // to solve some
																// problems with
																// QueryString.
				request.getSession().setAttribute("uri", uri);
				
				//Map<String, Integer> result = findDatasets(uri, request);
				Map<String, Integer> result = LuceneUtil.sortByComparator(findDatasets(uri, request), false, 5);
				
				if ((result != null) && (result.size() > 0)) {
					String json = "[";
					for (Map.Entry<String, Integer> elem : result.entrySet()) {
						String endPoint = elem.getKey();
						int dType = elem.getValue();
						json += ",{\"dataset\":\"" + endPoint + "\",\"CountDataType\":\"" + dType + "\"}";
					}
					json = json.replaceFirst(",", "");
					json += "]";
					response.getOutputStream().println(json);
				} else {
					response.getOutputStream().println("<h1>NOTHING !</h1>");
				}
				// response.getWriter().write(sameas.getSameAsURI(request.getParameter("uri"),true));
			} else if (request.getParameter("uris") != null) {
				findURIS(request, response);
			} else if (request.getParameter("link") != null) {
				findLinklion(request, response);
			} else if (request.getParameter("uri1") != null) {
				findWeb(request, response);
			} else if (request.getParameter("urihdt") != null) {
				findHdt(request, response);
			} else if (request.getParameter("statistics") != null) {
				findStatistics(request, response);
			} 
//				else if ((request.getParameter("dirHDT") != null) 
//					&& (request.getParameter("dirEndpoints") != null) 
//					&& (request.getParameter("dirDumps") != null)){
//				request.getSession().setAttribute("dirHDT", request.getParameter("dirHDT"));
//				request.getSession().setAttribute("dirEndpoints", request.getParameter("dirEndpoints"));
//				request.getSession().setAttribute("dirDumps", request.getParameter("dirDumps"));
//				
//				try {
//					HDTQueryMan.loadFileMap("md5HDT.csv");
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//				
//				response.sendRedirect("index.jsp");
//			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Map<String, Integer> findDatasets(String uri, HttpServletRequest request) throws IOException, ParseException {
		
//		String dirHDT = null;
//		String dirEndpoints = null;
//		String dirDumps = null;
//		//if(request.getSession().getAttribute("dirHDT") != null){
//		if(System.getProperty("dirHDT") != null){
//			//dirHDT = request.getSession().getAttribute("dirHDT").toString();
//			dirHDT = System.getProperty("dirHDT");
//		}
//		if(System.getProperty("dirEndpoints") != null){
//			//dirEndpoints = request.getSession().getAttribute("dirEndpoints").toString();
//			dirEndpoints = System.getProperty("dirEndpoints");
//		}
//		if(System.getProperty("dirDumps") != null){
//			//dirDumps = request.getSession().getAttribute("dirDumps").toString();
//			dirDumps = System.getProperty("dirDumps");
//		}
//		
//		//Map<String, Integer> result = HDTQueryMan.findDatasetsHDT(uri, dirHDT);
//		Set<String> sDirs = new HashSet<String>();
//		
//		String sHDT [] = dirHDT.split(",");
//		String sEndpoints [] = dirEndpoints.split(",");
//		String sDumps [] = dirDumps.split(",");
//		for (String dHDT : sHDT) {
//			sDirs.add(dHDT);
//		}
//		for (String dEndpoint : sEndpoints) {
//			sDirs.add(dEndpoint);
//		}
//		for (String dDump : sDumps) {
//			sDirs.add(dDump);
//		}
		Set<String> sDirs = new HashSet<String>();
		String dirInd = null;
		String dirHDT = null;
		if(System.getProperty("dirInd") != null){
			dirInd = System.getProperty("dirInd");
		}
		File fDir = new File(dirInd);
		String s[] = fDir.list();
		for (String dir : s) {
			if(dir.equals("hdtDatasets")){
				dirHDT = fDir.getAbsolutePath() + "/" + dir;
			}else{
				sDirs.add(fDir.getAbsolutePath() + "/" + dir);
			}
		}

		Map<String, Integer> result = LuceneUtil.search(uri, 1000, sDirs);
		result.putAll(HDTQueryMan.findDatasetsHDT(uri, dirHDT));
		
		return result;
	}

	private void findStatistics(HttpServletRequest request, HttpServletResponse response) throws IOException {
		long start = System.currentTimeMillis();
		String sDatasets = HDTQueryMan.printDatasets();
		//int dTypesDBpedia = EndPoint.getDataTypes(uri);
		//result.put("http://dbpedia.org/sparql", dTypesDBpedia);
		long totalTime = System.currentTimeMillis() - start;
		
		if ((sDatasets != null) && (sDatasets.length() > 0)) {
			response.getOutputStream().println("<table style=\"margin-left: auto; margin-right: auto;\" border=\"1\"> "
					+ "<tbody> <tr> <td><strong>Dataset</strong></td> <td><strong>Triples</strong></td>"
					+ "<td><strong>Datatypes/Literals</strong></td> <td><strong>Type</strong></td></tr>");
			
			try {
				response.getOutputStream().println(sDatasets);
			} catch (Exception e) {
				e.printStackTrace();
			}
			response.getOutputStream().println("</tbody></table>");
		} else {
			response.getOutputStream().println("<h1>NOTHING !</h1>");
		}
		response.getOutputStream().println("<br> TotalTime: " + totalTime + " ms"
				+ "<br>The data comes from the most useful/popular datasets from the <a href='http://linkeddata.org/'>http://linkeddata.org/</a>"
				+ " available in HDT format <a href='http://www.rdfhdt.org/datasets/'>http://www.rdfhdt.org/</a>");
	}

	private void findHdt(HttpServletRequest request, HttpServletResponse response) throws IOException, ParseException {
		String uri = request.getParameter("urihdt");
		request.getSession().setAttribute("urihdt", uri);
		int top = 10;
		try{
			top = Integer.parseInt(request.getParameter("top"));
		}catch(Exception ex){
			
		}
		
		long start = System.currentTimeMillis();
		
		Map<String, Integer> result1 = findDatasets(uri, request);
		
//		Map<String, Integer> result = 
//			     result1.entrySet().stream()
//			    .sorted(Entry.comparingByValue())
//			    .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
//			                              (e1, e2) -> e1, LinkedHashMap::new));
		
		Map<String, Integer> result = LuceneUtil.sortByComparator(result1, false, top);
		
		long totalTime = System.currentTimeMillis() - start;
		
		if ((result != null) && (result.size() > 0)) {
			response.getOutputStream().println(
					"<table border='1'> <tr> <th>Dataset</th> <th>Literals</th> <th>HDT</th> <th>Original file</th> </tr>");
			//response.getOutputStream().println("<script src=\"https://www.w3schools.com/lib/w3.js\"></script>");
//			response.getOutputStream().println(
//					"<table id=\"myTable\" border='1'> " + "<tr> " + "<th onclick=\"w3.sortHTML('#myTable', '.item', 'td:nth-child(1)')\" style=\"cursor:pointer\">Dataset</th> "
//							+ "<th onclick=\"w3.sortHTML('#myTable', '.item', 'td:nth-child(2)')\" style=\"cursor:pointer\">Country</th> " + "</tr>");
			//result.entrySet().stream().limit(5).forEach(elem -> {
			result.entrySet().forEach(elem -> {
				String dataset = elem.getKey();
				
				String md5 = "";
				String dsName = null;
				String dsHDT = "#";
				String imgHDT = "-";
				String imgFile = "<img src=\"http://st.depositphotos.com/1000715/125/i/950/depositphotos_1258962-Old-folder-with-stack-of-old-papers.jpg\"alt=\"Donwload the original file.\" width=\"30\" height=\"30\">";
				//if((dataset != null) && (dataset.length() > 40)){
				//String md5 = dataset.substring(0, dataset.indexOf("."));
				try{
					md5 = dataset.substring(34, dataset.indexOf("?"));
					dsName = HDTQueryMan.md5Names.get(md5);
					dsHDT = dataset;
					imgHDT = "<img src=\"https://dataweb.infor.uva.es/projects/hdt-mr/files/logo-hdt-75x75.png\"alt=\"Donwload the HDT file.\" width=\"30\" height=\"30\">";
					dataset = (dsName != null) ? dsName : dataset;
				}catch(Exception ex){
					//System.err.println(dataset + " -NON MD5HDT- ");
				}
				
				String urlDataset = null;
				//String urlDataset = "http://gaia.infor.uva.es/hdt/" + dataset + ".gz";
				if(dataset.startsWith("http") || dataset.startsWith("ftp"))
					urlDataset = dataset;
				else {
					if(dsName != null){
						urlDataset = "http://download.lodlaundromat.org/"+md5+"?type=hdt";
					}else{
						urlDataset = dataset;
					}
					
				}
				
				if(urlDataset.endsWith(".hdt")){
					urlDataset = "http://gaia.infor.uva.es/hdt/" + urlDataset + ".gz";
				}
				
				if((!dataset.startsWith("http")) || (!dataset.startsWith("ftp"))){
					dataset = urlDataset;
				}
				
				int dType = elem.getValue();
				try {
//					response.getOutputStream()
//					.println("<tr> " + "<td><a href='"+ urlDataset +"'>" + dataset + "</a></td> " + "<td>" + dType + "</td> <td><a href='"+ dsHDT +"'>"+ imgHDT +"</a></td></tr>");
					response.getOutputStream()
					.println("<tr> " + "<td>" + dataset + "</td> " + "<td>" + dType + "</td> <td><a href='"+ dsHDT +"'>"+ imgHDT +"</a></td><td><a href='"+ urlDataset +"'>"+ imgFile +"</a></td></tr>");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			response.getOutputStream().println("</table>");
			response.getOutputStream().println("<h3>URI: " + uri + "</h3> <br>Top "+ top +" from "+ result1.size() +" datasets.");
			//response.getOutputStream().println("<h3>Global: " + System.getProperty("nada") + "</h3>");
		} else {
			response.getOutputStream().println("<h1>NOTHING !</h1>");
		}
		response.getOutputStream().println("<br> TotalTime: " + totalTime + " ms "
				+ "<br> Time creating MD5HDT mapping: " + timeLoadMD5HDT + " ms"
				+ "<br>The data comes from the most useful/popular datasets from the <a href='http://linkeddata.org/'>http://linkeddata.org/</a>"
				+ " available in HDT format <a href='http://www.rdfhdt.org/datasets/'>http://www.rdfhdt.org/</a><br>"
				+ "<a href='./'>Back</a>");
	}

	private void findWeb(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String uri = request.getParameter("uri1");
		//uri = uri.replaceAll("123nada", "#").trim(); // to solve some problems
		final String uri1 = uri;												// with QueryString.
		request.getSession().setAttribute("uri1", uri);

		long start = System.currentTimeMillis();
		Map<String, Integer> result = DBUtil.findEndPoint(uri);
		long totalTime = System.currentTimeMillis() - start;
		totalTime = TimeUnit.MILLISECONDS.toMinutes(totalTime);
		
		if ((result != null) && (result.size() > 0)) {
			response.getOutputStream().println(
					"<table border='1'> " + "<tr> " + "<th>Dataset</th> " + "<th>Count DataType</th> " + "</tr>");
			result.entrySet().forEach(elem -> {
				String endPoint = elem.getKey();
				int dType = elem.getValue();
				try {
					String endPointS = endPoint + "?query=describe <" + URLEncoder.encode(uri1, "UTF-8") +">";
					response.getOutputStream()
					.println("<tr> " + "<td><a href='"+ endPointS +"'>" + endPoint + "</a></td> " + "<td>" + dType + "</td> " + "</tr>");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			response.getOutputStream().println("</table>");
		} else {
			response.getOutputStream().println("<h1>NOTHING !</h1>");
		}
		response.getOutputStream().println("<br> TotalTime: " + totalTime + " minutes"
				+ "<br>The data comes from <a href='http://lodstats.aksw.org/'>LODStats</a>"
				+ " and <a href='http://downloads.dbpedia.org/2016-10/core-i18n/en/'>DBpedia Dumps</a>");
	}

	private void findSQL(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String sql = request.getParameter("SQL");
		sql = sql.replaceAll("123nada", "#").trim(); // to solve some problems
														// with QueryString.
		request.getSession().setAttribute("sql", sql);

		Set<String> result = DBUtil.sendSQL(sql);

		if ((result != null) && (result.size() > 0)) {
			String json = "[";
			for (String elem : result) {
				json += ",{ \"elem\":\"" + elem + "\"}";
			}
			json = json.replaceFirst(",", "");
			json += "]";
			response.getOutputStream().println(json);
		} else {
			response.getOutputStream().println("<h1>NOTHING !</h1>");
		}

	}

	private void findURIS(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String uris = request.getParameter("uris");
		request.getSession().setAttribute("uris", uris);
		
		Set<String> setUris = Arrays.stream(uris.split(",")).collect(Collectors.toSet());
		
		String dirHDT = System.getProperty("user.home") + "/hdtDatasets";
		if(request.getSession().getAttribute("dirHDT") != null){
			dirHDT = request.getSession().getAttribute("dirHDT").toString();
		}		
		Map<String, Map<String, Integer>> results = HDTQueryMan.findDatasetsHDT(setUris, dirHDT);
		
		results.keySet().parallelStream().forEach(uri ->{
			Map<String, Integer> result = results.get(uri);
			String json = "[\"uri\":\"" + uri + "\"";
			if ((result != null) && (result.size() > 0)) {
				//String json = "[\"uri\":\"" + uri + "\"";
				for (Map.Entry<String, Integer> elem : result.entrySet()) {
					String endPoint = elem.getKey();
					int dType = elem.getValue();
					json += ",{\"EndPoint\":\"" + endPoint + "\",\"CountDataType\":\"" + dType + "\"}";
				}
				json = json.replaceFirst(",", "");
				json += "]";
				try {
					response.getOutputStream().println(json);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					json += "]";
					response.getOutputStream().println(json);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		});
		
	}
	
	private void findLinklion(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String link = request.getParameter("link");
		request.getSession().setAttribute("link", link);
		
		URL url = new URL(link);
		File fLink = new File("linkset.nt");
		FileUtils.copyURLToFile(url, fLink);
		
		String dirHDT = null;
		String dirEndpoints = null;
		String dirDumps = null;
		if(System.getProperty("dirHDT") != null){
			dirHDT = System.getProperty("dirHDT");
		}
		if(System.getProperty("dirEndpoints") != null){
			dirEndpoints = System.getProperty("dirEndpoints");
		}
		if(System.getProperty("dirDumps") != null){
			dirDumps = System.getProperty("dirDumps");
		}
		
		Set<String> sDirs = new HashSet<String>();
		String sHDT [] = dirHDT.split(",");
		for (String dHDT : sHDT) {
			sDirs.add(dHDT);
		}

		sDirs.add(dirDumps);
		sDirs.add(dirEndpoints);

		Set<WIMUri> results = LuceneUtil.search(fLink, 1000, sDirs);
			
		//String json = LuceneUtil.set2Json(results);
		String json = new Gson().toJson(results);
		
		response.getOutputStream().println(json);
	}
}
