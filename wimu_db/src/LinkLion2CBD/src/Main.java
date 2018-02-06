import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

import com.google.gson.Gson;

public class Main {

	static final String WIMUservice = "http://dmoussallem.ddns.net:1550/LinkLion2_WServ/Find?link=";
	// static final String WIMUservice =
	// "http://139.18.8.58:8080/LinkLion2_WServ/Find?link=";
	static final String linksetsRepo = "http://www.linklion.org/download/mapping/";
	static final boolean onlyTest = false;

	public static void main(String[] args) throws Exception {
		// process();
		processParallel();
	}

	public static void process() throws Exception {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		long start = System.currentTimeMillis();
		// String linkset =
		// "http://139.18.8.58:8080/LinkLion2_WServ/Find?link=http://www.linklion.org/download/mapping/sws.geonames.org---purl.org.nt";

		Set<String> linksets = getLinksets(linksetsRepo,onlyTest);
		PrintWriter writerS = new PrintWriter("cbdS.nt", "UTF-8");
		PrintWriter writerT = new PrintWriter("cbdT.nt", "UTF-8");

		Set<WIMUri> wimuURIs = new HashSet<WIMUri>();
		// linksets.parallelStream().forEach(linkset -> {
		linksets.forEach(linkset -> {
			try {
				wimuURIs.addAll(readJson(linkset));
				System.out.println("SUCCESS: " + linkset);
			} catch (Exception e) {
				System.out.println("FAIL: " + linkset + " ERROR: " + e.getMessage());
			}
		});
		// wimuURIs.parallelStream().forEach(wUri -> {
		wimuURIs.forEach(wUri -> {
			System.out.println(wUri);
			try {
				Map<Set<String>, Set<String>> mCBD_s_t = getAllCBD(wUri);
				// mCBD_s_t.entrySet().forEach(entry -> {
				mCBD_s_t.entrySet().parallelStream().forEach(entry -> {
					Set<String> cbdS = entry.getKey();
					Set<String> cbdT = entry.getValue();
					if (cbdS != null) {
						cbdS.parallelStream().forEach(tripleS -> {
							writerS.println(tripleS);
						});
					}
					if (cbdT != null) {
						cbdT.parallelStream().forEach(tripleT -> {
							writerT.println(tripleT);
						});
					}
				});
			} catch (Exception ex) {
				System.out.println(wUri + " Error: " + ex.getMessage());
			}
		});
		writerS.close();
		writerT.close();
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime: " + totalTime);
	}

	public static void processParallel() throws Exception {
		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
		System.out.println("Parallel version");

		long start = System.currentTimeMillis();
		// String linkset =
		// "http://139.18.8.58:8080/LinkLion2_WServ/Find?link=http://www.linklion.org/download/mapping/sws.geonames.org---purl.org.nt";
		Set<String> linksets = getLinksets(linksetsRepo,onlyTest);
		PrintWriter writerS = new PrintWriter("cbdS.nt", "UTF-8");
		PrintWriter writerT = new PrintWriter("cbdT.nt", "UTF-8");

		Set<WIMUri> wimuURIs = new HashSet<WIMUri>();
		System.out.println("Number of linksets: " + linksets.size());
		linksets.parallelStream().forEach(linkset -> {
			// linksets.forEach(linkset -> {
			try {
				wimuURIs.addAll(readJson(linkset));
				System.out.println("SUCCESS_1: " + linkset);
			} catch (Exception e) {
				System.out.println("FAIL: " + linkset + " ERROR: " + e.getMessage());
			}
		});
		System.out.println("Number of URIs to process: " + (wimuURIs.size() * 2));
		wimuURIs.parallelStream().forEach(wUri -> {
			// wimuURIs.forEach(wUri -> {
			System.out.println(wUri);
			try {
				Map<Set<String>, Set<String>> mCBD_s_t = getAllCBD(wUri);
				// mCBD_s_t.entrySet().forEach(entry -> {
				mCBD_s_t.entrySet().parallelStream().forEach(entry -> {
					Set<String> cbdS = entry.getKey();
					Set<String> cbdT = entry.getValue();
					if (cbdS != null) {
						cbdS.parallelStream().forEach(tripleS -> {
							writerS.println(tripleS);
						});
					}
					if (cbdT != null) {
						cbdT.parallelStream().forEach(tripleT -> {
							writerT.println(tripleT);
						});
					}
				});
			} catch (Exception ex) {
				System.out.println(wUri + " Error: " + ex.getMessage());
			}
		});
		writerS.close();
		writerT.close();
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("TotalTime: " + totalTime);
	}

	private static Set<String> getLinksets(String urlRepository, boolean bSample) {
		Set<String> linksets = new HashSet<String>();
		if (bSample) {
			linksets.add(WIMUservice + urlRepository + "sws.geonames.org---purl.org.nt");
			linksets.add(WIMUservice + urlRepository + "citeseer.rkbexplorer.com---nsf.rkbexplorer.com.nt");
			linksets.add(WIMUservice + urlRepository + "AGROVOC.nt---agclass.nal.usda.gov.nt");
		} else {
			try {
				org.jsoup.nodes.Document doc = Jsoup.connect(urlRepository).get();
				for (Element file : doc.select("a")) {
					String fName = file.attr("href");
					if (fName.endsWith(".nt"))
						linksets.add(WIMUservice + urlRepository + fName);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return linksets;
	}

	private static synchronized Map<Set<String>, Set<String>> getAllCBD(WIMUri wUri) throws Exception {
		Map<Set<String>, Set<String>> ret = new HashMap<Set<String>, Set<String>>();
		Set<String> cbdS = getCBD(wUri.getUriS(), wUri.getDatasetS(), wUri.getHdtS());
		Set<String> cbdT = getCBD(wUri.getUriT(), wUri.getDatasetT(), wUri.getHdtT());
		ret.put(cbdS, cbdT);
		return ret;
	}

	private static synchronized Set<String> getCBD(String uri, String urlDataset, String urlHDT) throws Exception {
		Set<String> cbd = null;
		if (urlHDT != null) {
			cbd = getCBDHDT(uri, urlHDT);
		}
		if ((cbd != null) && (cbd.size() < 1) && (urlDataset != null) && (urlDataset.length() > 1)) {
			System.out.println("NON HDT: " + urlDataset);
			// cbd = getCBDDataset(uri,urlDataset);
		}
		return cbd;
	}

	private static Set<String> getCBDDataset(String uri, String urlDataset) throws Exception {
		Set<String> ret = new HashSet<String>();
		throw new Exception("NEED TO IMPLEMENT");
	}

	private static Set<String> getCBDHDT(String uri, String urlHDT) throws IOException {
		Set<String> ret = new HashSet<String>();
		File file = new File("file.hdt");
		HDT hdt = null;
		try {
			URL url = new URL(urlHDT);
			FileUtils.copyURLToFile(url, file);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			String sparql = "Select * where {?s ?p ?o . filter(?s=<" + uri + "> || ?o=<" + uri + ">) }";

			Query query = QueryFactory.create(sparql);

			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			while (results.hasNext()) {
				QuerySolution thisRow = results.next();
				// uriDataset = thisRow.get("s").toString() + "\t" +
				// file.getDataset();
				String nTriple = "<" + thisRow.get("s").toString() + "> <" + thisRow.get("p").toString() + ">";
				if (thisRow.get("o").isLiteral()) {
					nTriple += " \"" + thisRow.get("o").asLiteral().toString() + "\"^^<"
							+ thisRow.get("o").asLiteral().getDatatypeURI() + "> .";
				} else {
					nTriple += " <" + thisRow.get("o").toString() + "> .";
				}
				ret.add(nTriple);
			}
			qe.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + urlHDT + " Error: " + e.getMessage());
		} finally {
			file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}

		return ret;
	}

	private static Set<WIMUri> readJson(String linksetURL) throws Exception {
		System.out.println("Processing linkset with WIMU: " + linksetURL);
		URL url = new URL(linksetURL);
		InputStreamReader reader = new InputStreamReader(url.openStream());
		WIMUri[] dto = new Gson().fromJson(reader, WIMUri[].class);
		Set<WIMUri> ret = Arrays.stream(dto).collect(Collectors.toCollection(HashSet::new));
		return ret;
	}
}
