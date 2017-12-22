package servlets;

import java.io.IOException;
import java.net.URLDecoder;
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

import org.apache.lucene.queryparser.classic.ParseException;

/**
 * Servlet implementation class Find
 */
public class Find extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Find() {
		super();
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
				
				Map<String, Integer> result = findDatasets(uri, request);
				
				if ((result != null) && (result.size() > 0)) {
					String json = "[";
					for (Map.Entry<String, Integer> elem : result.entrySet()) {
						String endPoint = elem.getKey();
						int dType = elem.getValue();
						json += ",{\"EndPoint\":\"" + endPoint + "\",\"CountDataType\":\"" + dType + "\"}";
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
			} else if (request.getParameter("endpoint") != null) {
				findURIS(request, response);
			} else if (request.getParameter("SQL") != null) {
				findSQL(request, response);
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
		String dirHDT = System.getProperty("user.home") + "/hdtDatasets";
		String dirEndpoints = System.getProperty("user.home") + "/endpoints";
		String dirDumps = System.getProperty("user.home") + "/dumps";
		if(request.getSession().getAttribute("dirHDT") != null){
			dirHDT = request.getSession().getAttribute("dirHDT").toString();
		}
		if(request.getSession().getAttribute("dirEndpoints") != null){
			dirEndpoints = request.getSession().getAttribute("dirEndpoints").toString();
		}
		if(request.getSession().getAttribute("dirDumps") != null){
			dirDumps = request.getSession().getAttribute("dirDumps").toString();
		}
		
		Map<String, Integer> result = HDTQueryMan.findDatasetsHDT(uri, dirHDT);
		Set<String> sDirs = new HashSet<String>();
		sDirs.add(dirDumps);
		sDirs.add(dirEndpoints);
		result.putAll(LuceneSearch.search(uri, 1000, sDirs));
		
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
		long start = System.currentTimeMillis();
		
		Map<String, Integer> result = findDatasets(uri, request);

		long totalTime = System.currentTimeMillis() - start;
		
		if ((result != null) && (result.size() > 0)) {
			response.getOutputStream().println(
					"<table border='1'> " + "<tr> " + "<th>Dataset</th> " + "<th>Count DataType</th> " + "</tr>");
			result.entrySet().forEach(elem -> {
				String dataset = elem.getKey();
				
				String md5 = dataset.substring(0, dataset.indexOf("."));
				String dsName = HDTQueryMan.md5Names.get(md5);
				dataset = (dsName != null) ? dsName : dataset;
				
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
						
				int dType = elem.getValue();
				try {
					response.getOutputStream()
					.println("<tr> " + "<td><a href='"+ urlDataset +"'>" + dataset + "</a></td> " + "<td>" + dType + "</td> " + "</tr>");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			response.getOutputStream().println("</table>");
			response.getOutputStream().println("<h3>URI: " + uri + "</h3>");
		} else {
			response.getOutputStream().println("<h1>NOTHING !</h1>");
		}
		response.getOutputStream().println("<br> TotalTime: " + totalTime + " ms"
				+ "<br>The data comes from the most useful/popular datasets from the <a href='http://linkeddata.org/'>http://linkeddata.org/</a>"
				+ " available in HDT format <a href='http://www.rdfhdt.org/datasets/'>http://www.rdfhdt.org/</a><br>"
				+ "<a href='/LinkLion2_WServ/'>Back</a>");
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

}
