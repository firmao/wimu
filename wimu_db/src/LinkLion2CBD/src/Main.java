import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;
import org.tukaani.xz.XZInputStream;

import com.google.gson.Gson;

public class Main {

	//static final String WIMUservice = "http://dmoussallem.ddns.net:1550/LinkLion2_WServ/Find?uri=";
	//static final String WIMUservice = "http://139.18.2.39:8122/Find?uri="; // akswnc9
	
	static final String WIMUservice = "http://localhost:8080/LinkLion2_WServ/Find?uri=";
	// static final String WIMUservice =
	// "http://dmoussallem.ddns.net:1550/LinkLion2_WServ/Find?link=";
	// static final String WIMUservice =
	// "http://139.18.8.58:8080/LinkLion2_WServ/Find?link=";
	static final String linksetsRepo = "http://www.linklion.org/download/mapping/";
	static final boolean onlyTest = true;
	
	static final Set<String> setProcessedSources = new HashSet<String>();
	static final Set<String> setProcessedTargets = new HashSet<String>();

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

		Set<String> linksets = getLinksets(linksetsRepo, onlyTest);
		PrintWriter writerS = new PrintWriter("cbdS.nt", "UTF-8");
		PrintWriter writerT = new PrintWriter("cbdT.nt", "UTF-8");

		Set<WIMUri> wimuURIs = new HashSet<WIMUri>();
		// linksets.parallelStream().forEach(linkset -> {
		linksets.forEach(linkset -> {
			try {
				wimuURIs.addAll(readJsonLinkset(linkset));
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
		System.out.println("Parallel version: Starting to using WIMU to find datasets from URIs.");

		int processors = Runtime.getRuntime().availableProcessors();
		System.out.println("Core processors: " + processors);
		
		long start = System.currentTimeMillis();
		// String linkset =
		// "http://139.18.8.58:8080/LinkLion2_WServ/Find?link=http://www.linklion.org/download/mapping/sws.geonames.org---purl.org.nt";
		Set<String> linksets = getLinksets(linksetsRepo, onlyTest);
		System.out.println("Number of linksets: " + linksets.size());
		PrintWriter writerS = new PrintWriter("cbdS.nt", "UTF-8");
		PrintWriter writerT = new PrintWriter("cbdT.nt", "UTF-8");

		Set<WIMUri> wimuURIs = new HashSet<WIMUri>();
		Map<String, WIMUDataset> mURIs = getURILinklion(linksets);
		linksets.parallelStream().forEach(linkset -> {
		//linksets.forEach(linkset -> {
			try {
				wimuURIs.addAll(processWimuLinkset(linkset, mURIs));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

		System.out.println("Number of URIs to process: " + mURIs.size());
		System.out.println("Starting to extract CBDs from datasets (HDT and usual RDF): " + mURIs.size());
		
		wimuURIs.parallelStream().forEach(wUri -> {
		//wimuURIs.forEach(wUri -> {
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

	private static Set<WIMUri> processWimuLinkset(String linkset, Map<String, WIMUDataset> mURIs) throws IOException {
		Set<WIMUri> ret = new HashSet<WIMUri>();
		URL url = new URL(linkset);
		File fLink = new File("linkset.nt");
		FileUtils.copyURLToFile(url, fLink);
		ret.addAll(processLinksetMap(fLink, mURIs));
		fLink.delete();
		return ret;
	}

	private static Set<WIMUri> processLinksetMap(File fLink, Map<String, WIMUDataset> mURIs) {
		Set<WIMUri> ret = new HashSet<WIMUri>();
		try {
			StreamRDF reader = new StreamRDF() {

				@Override
				public void base(String arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void finish() {
					// TODO Auto-generated method stub

				}

				@Override
				public void prefix(String arg0, String arg1) {
					// TODO Auto-generated method stub

				}

				@Override
				public void quad(Quad arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void start() {
					// TODO Auto-generated method stub

				}

				@Override
				public synchronized void triple(Triple triple) {
					String source = triple.getSubject().toString();
					String target = triple.getObject().toString();
					WIMUri wURI = new WIMUri(source, target);
					String dataset = null;
					try {
						dataset = mURIs.get(source).getDataset();
						wURI.setDatasetS(dataset);
					} catch (Exception e) {
					}
					try {
						dataset = mURIs.get(source).getHdt();
						wURI.setHdtS(dataset);
					} catch (Exception e) {
					}
					try {
						dataset = mURIs.get(target).getDataset();
						wURI.setDatasetT(dataset);
					} catch (Exception e) {
					}
					try {
						dataset = mURIs.get(target).getHdt();
						wURI.setHdtT(dataset);
					} catch (Exception e) {
					}
					ret.add(wURI);
				}
			};
			RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.NT);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return ret;
	}

	private static Map<String, WIMUDataset> getURILinklion(Set<String> linksets) {
		Map<String, WIMUDataset> ret = new HashMap<String, WIMUDataset>();
		linksets.parallelStream().forEach(linkset -> {
		//linksets.forEach(linkset -> {
			try {
				URL url = new URL(linkset);
				File fLink = new File("linkset.nt");
				FileUtils.copyURLToFile(url, fLink);
				ret.putAll(processLinkset(fLink));
				fLink.delete();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		return ret;
	}

	private static Map<String, WIMUDataset> processLinkset(File fLink) {
		Map<String, WIMUDataset> ret = new HashMap<String, WIMUDataset>();
		StreamRDF reader = new StreamRDF() {

			@Override
			public void base(String arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void finish() {
				// TODO Auto-generated method stub

			}

			@Override
			public void prefix(String arg0, String arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void quad(Quad arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void start() {
				// TODO Auto-generated method stub

			}

			@Override
			public synchronized void triple(Triple triple) {
				String dSource = triple.getSubject().toString();
				String dTarget = triple.getObject().toString();
				String source=null;
				try {
					source = URLEncoder.encode(triple.getSubject().toString(), "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				String target=null;
				try {
					target = URLEncoder.encode(triple.getObject().toString(), "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				};
				WIMUDataset dS = null;
				try {
					if (!ret.containsKey(source)) {
						dS = readJsonURI(WIMUservice + source);
						source = URLDecoder.decode(source, "UTF-8");
						ret.put(source, dS);
					}
				} catch (Exception e) {
					System.out.println("No dataset for URI: " + dSource + " ERROR: " + e.getMessage());
				}
				WIMUDataset dT = null;
				try {
					if (!ret.containsKey(target)) {
						dT = readJsonURI(WIMUservice + target);
						target = URLDecoder.decode(target, "UTF-8");
						ret.put(target, dT);
					}
				} catch (Exception e) {
					System.out.println("No dataset for URI: " + dTarget + " ERROR: " + e.getMessage());
				}
			}
		};
		RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.NT);
		return ret;
	}

	private static Set<String> getLinksets(String urlRepository, boolean bSample) {
		Set<String> linksets = new HashSet<String>();
		if (bSample) {
			linksets.add(urlRepository + "sws.geonames.org---purl.org.nt");
			linksets.add(urlRepository + "citeseer.rkbexplorer.com---nsf.rkbexplorer.com.nt");
			linksets.add(urlRepository + "AGROVOC.nt---agclass.nal.usda.gov.nt");
			linksets.add(urlRepository + "purl.org---xmlns.com.nt"); // just 1 link
		} else {
			try {
				org.jsoup.nodes.Document doc = Jsoup.connect(urlRepository).get();
				for (Element file : doc.select("a")) {
					String fName = file.attr("href");
					if (fName.endsWith(".nt")) {
						// linksets.add(WIMUservice + urlRepository + fName);
						linksets.add(urlRepository + fName);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return linksets;
	}

	private static synchronized Map<Set<String>, Set<String>> getAllCBD(WIMUri wUri) throws Exception {
		Map<Set<String>, Set<String>> ret = new HashMap<Set<String>, Set<String>>();
		Set<String> cbdS = null;
		Set<String> cbdT = null;	
		if(!setProcessedSources.contains(wUri.getUriS())){
			cbdS = getCBD(wUri.getUriS(), wUri.getDatasetS(), wUri.getHdtS());
			setProcessedSources.add(wUri.getUriS());
		}
		if(!setProcessedTargets.contains(wUri.getUriS())){
			cbdT = getCBD(wUri.getUriT(), wUri.getDatasetT(), wUri.getHdtT());
			setProcessedTargets.add(wUri.getUriT());
		}
		ret.put(cbdS, cbdT);
		return ret;
	}

	private static synchronized Set<String> getCBD(String uri, String urlDataset, String urlHDT) throws Exception {
		Set<String> cbd = null;
		if (urlHDT != null) {
			cbd = getCBDHDT(uri, urlHDT);
		}
		if ((cbd != null) && (cbd.size() < 1) && (urlDataset != null) && (urlDataset.length() > 1)) {
			//System.out.println("<IMPLEMENT !!!>NON HDT: " + urlDataset);
			cbd = getCBDDataset(uri,urlDataset);
		}
		return cbd;
	}

	private static Set<String> getCBDDataset(String uri, String urlDataset) throws Exception {
		Set<String> ret = new HashSet<String>();
		File file = null;
		try {
			URL url = new URL(urlDataset);
			file = new File(getURLFileName(url));
			FileUtils.copyURLToFile(url, file);
			file = unconpress(file);
			StreamRDF reader = new StreamRDF() {

				@Override
				public void base(String arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void finish() {
					// TODO Auto-generated method stub

				}

				@Override
				public void prefix(String arg0, String arg1) {
					// TODO Auto-generated method stub

				}

				@Override
				public void quad(Quad arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void start() {
					// TODO Auto-generated method stub

				}

				@Override
				public synchronized void triple(Triple triple) {
					String predicate = triple.getPredicate().getURI();
					if(predicate.endsWith("rdf-syntax-ns#type")){
						predicate = "http://www.w3.org/2002/07/owl#Thing";
					}
					if(predicate.endsWith("sameAs")) return;
					String nTriple = "<" + triple.getSubject().getURI() + "> <" + predicate + ">";
					if (triple.getObject().isLiteral()) {
						nTriple += " \"" + triple.getObject().getLiteral().toString() + "\"^^<"
								+ triple.getObject().getLiteral().getDatatypeURI() + "> .";
					} else {
						nTriple += " <" + triple.getObject().getURI() + "> .";
					}
					ret.add(nTriple);
				}
			};
			RDFParserBuilder a = RDFParserBuilder.create();
			if (file.getName().endsWith(".tql")) {
				//RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NQUADS);
				a.forceLang(Lang.NQUADS);
			} else if (file.getName().endsWith(".ttl")) {
				//RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.TTL);
				a.forceLang(Lang.TTL);
			} else if (file.getName().endsWith(".nt")) {
				//RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NT);
				a.forceLang(Lang.NT);
			} else if (file.getName().endsWith(".nq")) {
				//RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NQ);
				a.forceLang(Lang.NQ);
			} else {
				//RDFDataMgr.parse(reader, fUnzip.getAbsolutePath());
				a.forceLang(Lang.RDFXML);
			}
			Scanner in = null;
			try {
				in = new Scanner(file);
				while(in.hasNextLine()) {
					a.source(new StringReader(in.nextLine()));
					try {
						a.parse(reader);
					} catch (Exception e) {
						//e.printStackTrace();
					}
				}
				in.close();
			} catch (FileNotFoundException e) {
				//e.printStackTrace();
			}
			file.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return ret;
		//throw new Exception("NEED TO IMPLEMENT");
	}
	
	private static String toNTNotation(RDFNode s, RDFNode p, RDFNode o) {
		String nTriple = "<" + s.toString() + "> <" + p.toString() + ">";
		if (o.isLiteral()) {
			nTriple += " \"" + o.asLiteral().toString() + "\"^^<"
					+ o.asLiteral().getDatatypeURI() + "> .";
		} else {
			nTriple += " <" + o.toString() + "> .";
		}
		return nTriple;
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

			HashSet<String> instances = new HashSet<>();
			while (results.hasNext()) {
				QuerySolution thisRow = results.next();
				String predicate = thisRow.get("p").toString();
				if(predicate.endsWith("sameAs")) continue;
				if(thisRow.get("o").toString().startsWith("http://lodlaundromat.org/.well-known/genid")) continue;
				if(thisRow.get("s").toString().startsWith("http://lodlaundromat.org/.well-known/genid")) continue;
				// uriDataset = thisRow.get("s").toString() + "\t" +
				// file.getDataset();
				//String object = thisRow.get("o").toString();
				if(predicate.equals(RDF.type.getURI())) {
					boolean firstVisit = instances.add(thisRow.get("s").toString());
					if(firstVisit)
						ret.add(toNTNotation(thisRow.get("s"), RDF.type, OWL.Thing));
				}
				String nTriple = toNTNotation(thisRow.get("s"), thisRow.get("p"), thisRow.get("o"));
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

	private static Set<WIMUri> readJsonLinkset(String linksetURL) throws Exception {
		System.out.println("Processing linkset with WIMU: " + linksetURL);
		URL url = new URL(linksetURL);
		InputStreamReader reader = new InputStreamReader(url.openStream());
		WIMUri[] dto = new Gson().fromJson(reader, WIMUri[].class);
		Set<WIMUri> ret = Arrays.stream(dto).collect(Collectors.toCollection(HashSet::new));
		return ret;
	}

	private static WIMUDataset readJsonURI(String uri) throws Exception {
		System.out.println("Processing: " + uri);
		URL url = new URL(uri);
		InputStreamReader reader = new InputStreamReader(url.openStream());
		WIMUDataset wData = new Gson().fromJson(reader, WIMUDataset[].class)[0];
		if (wData.getDataset().endsWith("hdt")) {
			wData.setHdt(wData.getDataset());
			wData.setDataset(null);
		}

		return wData;
	}
	
	private static ResultComp compareLinkSets(File fLinkLionSet, File fWombatSet){
		ResultComp res = new ResultComp();
		//Map<String, Set<String>> mLinkLion = getGraph(fLinkLionSet); 
		StreamRDF reader = new StreamRDF() {

			@Override
			public void base(String arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void finish() {
				// TODO Auto-generated method stub

			}

			@Override
			public void prefix(String arg0, String arg1) {
				// TODO Auto-generated method stub

			}

			@Override
			public void quad(Quad arg0) {
				// TODO Auto-generated method stub

			}

			@Override
			public void start() {
				// TODO Auto-generated method stub

			}

			@Override
			public synchronized void triple(Triple triple) {
				String dSource = triple.getSubject().toString();
				String dTarget = triple.getObject().toString();
				//if()
			}
		};
		RDFDataMgr.parse(reader, fWombatSet.getAbsolutePath(), Lang.NT);
		
		try{
			fWombatSet.delete();
			fLinkLionSet.delete();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		return res;
	}
	
	private static File unconpress(File file) {
		File ret = file;
		try {
			File fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new File(file.getName().replaceAll(".bz2", ""));
			else if (file.getName().endsWith(".xz"))
				fUnzip = new File(file.getName().replaceAll(".xz", ""));
			else if (file.getName().endsWith(".zip"))
				fUnzip = new File(file.getName().replaceAll(".zip", ""));
			else if (file.getName().endsWith(".tar.gz"))
				fUnzip = new File(file.getName().replaceAll(".tar.gz", ""));
			else
				return file;

			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			FileOutputStream out = new FileOutputStream(fUnzip);

			if (file.getName().endsWith(".bz2")) {
				BZip2CompressorInputStream bz2In = new BZip2CompressorInputStream(in);
				synchronized (bz2In) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = bz2In.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					bz2In.close();
				}
			} else if (file.getName().endsWith(".xz")) {
				XZInputStream xzIn = new XZInputStream(in);
				synchronized (xzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = xzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					xzIn.close();
				}
			} else if (file.getName().endsWith(".zip")) {
				ZipArchiveInputStream zipIn = new ZipArchiveInputStream(in);
				synchronized (zipIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = zipIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					zipIn.close();
				}
			} else if (file.getName().endsWith(".tar.gz")) {
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
				synchronized (gzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = gzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					gzIn.close();
				}
			}

			file.delete();

			if (fUnzip != null)
				ret = fUnzip;
		} catch (Exception ex) {
			ret = file;
		}
		return ret;
	}
	
	public static String getURLFileName(URL pURL) {
		String[] str = pURL.getFile().split("/");
		return str[str.length - 1];
	}
}
