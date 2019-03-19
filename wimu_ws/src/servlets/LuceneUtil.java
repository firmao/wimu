package servlets;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonException;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class LuceneUtil {

	// Lucene
	public static final String N_TRIPLES = "NTriples";
	public static final String TTL = "ttl";
	public static final String TSV = "tsv";
	public static final Version LUCENE_VERSION = Version.LUCENE_44;
	private static Analyzer urlAnalyzer;
	private static DirectoryReader ireader;
	private static IndexWriter iwriter;
	private static MMapDirectory directory;

	public static void main(String[] args) throws IOException {
		// HDTQueryMan.loadFileMap("md5HDT.csv");
		// String dirHDT = System.getProperty("user.home") + "/hdtDatasets";
		// // String dirHDT = "/media/andre/TOSHIBA\\ EXT/hdtAmazon/hdtFiles/";
		// // createHDTLuceneIndex("dirLuceneHDT", new File(dirHDT));
		// File f = new File("/media/andre/DATA/linux/linklion2/f100.csv");
		// processFileBatch(f, "NO IP");
		//getLODdataJson("http://dbpedia.org/resource/Leipzig");
		System.out.println(queryLOD_a_lot("http://viaf.org/viaf/59183456"));
	}

	private static void processFileBatch(File pFile, String ipCli) throws IOException {
		PrintWriter writer = new PrintWriter("/media/andre/DATA/linux/linklion2/p100.csv", "UTF-8");
		List<String> lstLines = FileUtils.readLines(pFile, "UTF-8");
		Set<String> dirs = new HashSet<String>();
		dirs.add(System.getProperty("user.home") + "/luceneDirs/luceneDirHDT_");
		dirs.add("/media/andre/DATA/linux/linklion2/luceneDirs/luceneDirHDT_1");
		dirs.add("/media/andre/DATA/linux/linklion2/luceneDirs/luceneDirHDT_2");
		dirs.add("/media/andre/DATA/linux/linklion2/luceneDirs/luceneDirHDT_3");
		dirs.add(System.getProperty("user.home") + "/luceneDirs/endpoints");
		dirs.add(System.getProperty("user.home") + "/luceneDirs/lodStatsDumps");
		String dHDTFiles = System.getProperty("user.home") + "/hdtDatasets";
		lstLines.forEach(line -> {
			try {
				Map<String, Integer> map = search(line, 1000, dirs, ipCli);
				map.putAll(HDTQueryMan.findDatasetsHDT(line, dHDTFiles));
				Map<String, Integer> mRes = sortByComparator(map, false, 1);
				if (mRes.size() > 0) {
					String dataset = mRes.keySet().iterator().next();
					System.out.println(line + "\t" + dataset);
					writer.println(line + "\t" + dataset);
				}
			} catch (IOException | ParseException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		});
		writer.close();
	}

	private static void createHDTLuceneIndex(String dirLucene, File dirHDT) throws IOException {
		long start = System.currentTimeMillis();
		createLuceneDir(dirLucene);
		Set<File> datasets = null;
		try {
			datasets = Files.walk(Paths.get(dirHDT.getPath())).filter(Files::isRegularFile).map(Path::toFile)
					.collect(Collectors.toSet());
			datasets.forEach(dataset -> {
				// datasets.parallelStream().forEach(dataset -> {
				String uriDataset = null;
				HashMap<String, Integer> mDatatypeTriples = new HashMap<String, Integer>();
				int cDtypes = 0;
				HDT hdt = null;
				try {
					hdt = HDTManager.mapHDT(dataset.getAbsolutePath(), null);
					HDTGraph graph = new HDTGraph(hdt);
					Model model = new ModelCom(graph);
					String sparql = "Select * where {?s ?p ?o . filter(isLiteral(?o)) }";
					org.apache.jena.query.Query query = QueryFactory.create(sparql);
					QueryExecution qe = QueryExecutionFactory.create(query, model);
					ResultSet results = qe.execSelect();
					while (results.hasNext()) {
						QuerySolution thisRow = results.next();
						uriDataset = thisRow.get("s").toString() + "\t" + dataset.getName();
						if (mDatatypeTriples.containsKey(uriDataset)) {
							cDtypes = mDatatypeTriples.get(uriDataset) + 1;
							mDatatypeTriples.put(uriDataset, cDtypes);
						} else {
							mDatatypeTriples.put(uriDataset, 1);
						}
					}
					qe.close();
					insertLucene(mDatatypeTriples);
				} catch (Exception ioe) {
					// TODO Auto-generated catch block
					ioe.printStackTrace();
				}
				System.out.println(dataset.getAbsolutePath());
			});

			if (iwriter != null) {
				iwriter.close();
			}
			if (ireader != null) {
				ireader.close();
			}
			if (directory != null) {
				directory.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		long total = System.currentTimeMillis() - start;
		System.out.println("Total time: " + total);
		System.out.println("HDT files: " + datasets.size());
	}

	private static void createLuceneDir(String dirLucene) throws IOException {
		File indexDirectory = new File(dirLucene);
		indexDirectory.mkdir();
		directory = new MMapDirectory(indexDirectory);
		System.out.println("Created Lucene dir: " + indexDirectory.getAbsolutePath());
		urlAnalyzer = new SimpleAnalyzer(LUCENE_VERSION);
		Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
		mapping.put("uri", urlAnalyzer);
		mapping.put("dataset_dtype", urlAnalyzer);
		PerFieldAnalyzerWrapper perFieldAnalyzer = new PerFieldAnalyzerWrapper(urlAnalyzer, mapping);
		IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, perFieldAnalyzer);
		iwriter = new IndexWriter(directory, config);

		// iwriter.deleteAll();
		//
		iwriter.commit();
	}

	private static void insertLucene(HashMap<String, Integer> mDatatypeTriples2) throws IOException {
		System.out.println("Inserting Lucene " + mDatatypeTriples2.size() + " dataType triples");
		mDatatypeTriples2.entrySet().parallelStream().forEach(elem -> {
			// mDatatypeTriples2.entrySet().forEach(elem -> {
			String uriDs = elem.getKey();
			int dTypes = elem.getValue();

			String s[] = uriDs.split("\t");
			String uri = s[0];
			String dataset = s[1];
			try {
				Document doc = new Document();
				doc.add(new StringField("uri", uri, Store.YES));
				doc.add(new StringField("dataset_dtype", dataset + "\t" + dTypes, Store.YES));
				iwriter.addDocument(doc);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		});
		// iwriter.close();
	}

	public static Map<String, Integer> search(String uri, int maxResults, Set<String> dirs, String ipCli)
			throws IOException, ParseException {
		LogURI(uri, ipCli);
		ConcurrentHashMap<String, Integer> mResults = new ConcurrentHashMap<String, Integer>();
		dirs.parallelStream().forEach(dir -> {
			try {
				// System.out.println("Lucene dir: " + dir);
				File indexDirectory = new File(dir);
				MMapDirectory directory = new MMapDirectory(indexDirectory);
				DirectoryReader ireader = DirectoryReader.open(directory);
				IndexSearcher isearcher = new IndexSearcher(ireader);
				BooleanQuery bq = new BooleanQuery();
				Query q = new TermQuery(new Term("uri", uri.trim()));
				bq.add(q, BooleanClause.Occur.MUST);
				TopScoreDocCollector collector = TopScoreDocCollector.create(maxResults, true);
				isearcher.search(bq, collector);
				ScoreDoc[] hits = collector.topDocs().scoreDocs;

				for (int i = 0; i < hits.length; i++) {
					Document hitDoc = isearcher.doc(hits[i].doc);
					String s[] = hitDoc.get("dataset_dtype").split("\t");
					String dataset = s[0];
					String dtype = s[1];
					if (mResults.containsKey(dataset)) {
						int dtypes = mResults.get(dataset);
						mResults.put(dataset, dtypes + Integer.parseInt(dtype));
					} else {
						mResults.put(dataset, Integer.parseInt(dtype));
					}
				}
				// mResults.forEach((ds, dTypes) -> {
				// System.out.println(ds + "\t" + dTypes);
				// });
				if (ireader != null) {
					ireader.close();
				}
				if (directory != null) {
					directory.close();
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}
		});
		if (mResults.size() < 1) {
			mResults.putAll(getLODdataJson(uri));
		}
		return mResults;
	}

	public static ConcurrentHashMap<String, Integer> getLODdataJson(String uri) {
		ConcurrentHashMap<String, Integer> ret = new ConcurrentHashMap<String, Integer>();
		// read Json from: https://lod-cloud.net/lod-data.json 
		//or https://raw.githubusercontent.com/AxelPolleres/LODanalysis/master/lod-data.json
		ret.putAll(queryLOD_a_lot(uri));
		if (ret.size() > 0) {
			return ret;
		}

		String jSonURL = "https://lod-cloud.net/lod-data.json";
		if(!isGoodURL(jSonURL)){
			jSonURL = "https://raw.githubusercontent.com/dice-group/wimu/master/lod-data.json";
		}
		System.out.println("Using: " + jSonURL);
		try {
			URL url = new URL(jSonURL);
			InputStreamReader reader = null;
			try {
				reader = new InputStreamReader(url.openStream());
			} catch (Exception e) {
				Thread.sleep(5000);
				reader = new InputStreamReader(url.openStream());
			}
			Gson gson = new Gson();
			Type t = new TypeToken<Map<String, Object>>() {
			}.getType();
			Map<String, Object> map = gson.fromJson(reader, t);
			String domain = getDomainName(uri);
			// String domain = domain1.substring(0, domain1.indexOf("."));
			map.forEach((x, y) -> {
				// System.out.println("key : " + x + " , value : " + y);
				// if(x.contains(domain)){
				Map<String, Object> m1 = (Map<String, Object>) map.get(x);
				List<Map<String, Object>> lst = (List<Map<String, Object>>) m1.get("sparql");
				for (Map<String, Object> m2 : lst) {
					//System.out.println(m2.values());
					if ((m2.get("access_url").toString().contains(domain)) && (m2.get("status").equals("OK"))) {
						ret.put(m2.get("access_url").toString(), -1);
					}
				}

				// ret.put("endpoint", 0);
				// }
			});
			// LODbean[] wData = new Gson().fromJson(reader, LODbean[].class);
			// for (LODbean wDs : wData) {
			// // ret = wDs;
			// break;
			// }
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		// System.out.println(ret);
		return ret;
	}
	
	private static boolean isGoodURL(String url) {
		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setRequestMethod("HEAD");
			int responseCode = connection.getResponseCode();
			if ((responseCode == 200) || (responseCode == 400)) {
				return true;
			} else
				return false;
		} catch (Exception e) {
			return false;
		}
	}

	private static ConcurrentHashMap<String, Integer> queryLOD_a_lot(String uri) {
		ConcurrentHashMap<String, Integer> ret = new ConcurrentHashMap<String, Integer>();
		try {
			URL url = new URL(
					"https://hdt.lod.labs.vu.nl/triple?g=%3Chttps%3A//hdt.lod.labs.vu.nl/graph/LOD-a-lot%3E&s=%3C" + uri
							+ "%3E");
			BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

			if (in.readLine() != null) {
				ret.put("https://hdt.lod.labs.vu.nl/", 0);
				return ret;
			}

			url = new URL(
					"https://hdt.lod.labs.vu.nl/triple?g=%3Chttps%3A//hdt.lod.labs.vu.nl/graph/LOD-a-lot%3E&o=%3C" + uri
							+ "%3E");
			in = new BufferedReader(new InputStreamReader(url.openStream()));

			if (in.readLine() != null) {
				ret.put("https://hdt.lod.labs.vu.nl/", 0);
				return ret;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// ret.put(m2.get("access_url").toString(), -1);

		return ret;
	}

	public static String getDomainName(String url) throws URISyntaxException {
		URI uri = new URI(url);
		String domain = uri.getHost();
		// domain = domain.startsWith("www.") ? domain.substring(4) : domain;
		// domain = domain.substring(0, domain.indexOf("."));
		// return domain;
		return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

	private static void LogURI(String uri, String ipCli) {
		File file = new File("LogURI.tsv");
		String msg = "";
		String date = LocalDateTime.now().toString();
		msg += "[" + date + "]\t" + uri + "\t" + ipCli + "\n";
		try {
			if (!file.exists()) {
				file.createNewFile();
				System.out.println("LOGURI created at: " + file.getAbsolutePath());
			}
			//transform in MB
			long sizeInMb = file.length() / (1024 * 1024);
			if(sizeInMb > 10){
				file.renameTo(new File(date.substring(0, 10) + file.getName()));
				file = new File("LogURI.tsv");
				file.createNewFile();
				System.out.println("New LOGURI created at: " + file.getAbsolutePath());
			}
			// 3rd parameter boolean append = true
			FileUtils.writeStringToFile(file, msg, Charset.defaultCharset(), true);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static Map<String, Integer> search(String uri, int maxResults, String luceneDir, String ipCli)
			throws IOException, ParseException {
		LogURI(uri, ipCli);
		ConcurrentHashMap<String, Integer> mResults = new ConcurrentHashMap<String, Integer>();
		File indexDirectory = new File(luceneDir);
		MMapDirectory directory = new MMapDirectory(indexDirectory);
		DirectoryReader ireader = DirectoryReader.open(directory);
		IndexSearcher isearcher = new IndexSearcher(ireader);
		BooleanQuery bq = new BooleanQuery();
		Query q = new TermQuery(new Term("uri", uri.trim()));
		bq.add(q, BooleanClause.Occur.MUST);
		TopScoreDocCollector collector = TopScoreDocCollector.create(maxResults, true);
		isearcher.search(bq, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		for (int i = 0; i < hits.length; i++) {
			Document hitDoc = isearcher.doc(hits[i].doc);
			String s[] = hitDoc.get("dataset_dtype").split("\t");
			String dataset = s[0];
			String dtype = s[1];
			if (mResults.containsKey(dataset)) {
				int dtypes = mResults.get(dataset);
				mResults.put(dataset, dtypes + Integer.parseInt(dtype));
			} else {
				mResults.put(dataset, Integer.parseInt(dtype));
			}
		}
		// mResults.forEach((ds, dTypes) -> {
		// System.out.println(ds + "\t" + dTypes);
		// });
		if (ireader != null) {
			ireader.close();
		}
		if (directory != null) {
			directory.close();
		}
		if (mResults.size() < 1) {
			mResults.putAll(getLODdataJson(uri));
		}

		return mResults;
	}

	public static Set<WIMUri> search(File fLink, int maxResults, Set<String> dirs, String dirHDT, String ipCli) {
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
					try {
						Map<String, Integer> mHDTs = HDTQueryMan.findDatasetsHDT(source, dirHDT);
						Map<String, Integer> mHDTt = HDTQueryMan.findDatasetsHDT(target, dirHDT);
						mHDTs.putAll(search(source, maxResults, dirs, ipCli));
						mHDTt.putAll(search(target, maxResults, dirs, ipCli));
						Map<String, Integer> mSource = sortByComparator(mHDTs, false, 1);
						Map<String, Integer> mTarget = sortByComparator(mHDTt, false, 1);
						String datasetS = "";
						String datasetT = "";
						String md5 = null;
						try {
							datasetS = mSource.keySet().iterator().next();
							if (datasetS.contains("dbpedia") && (datasetS.contains("anchor_text"))) {
								throw new Exception("dbpedia:anchor_text not allowed in WIMU");
							}
							wURI.setcDatatypesS(mSource.values().iterator().next().intValue());
							md5 = datasetS.substring(34, datasetS.indexOf("?"));
							wURI.setDatasetS(HDTQueryMan.md5Names.get(md5));
							wURI.setHdtS(datasetS);
						} catch (Exception ex) {
							wURI.setDatasetS(datasetS);
						}
						try {
							datasetT = mTarget.keySet().iterator().next();
							if (datasetT.contains("dbpedia") && (datasetT.contains("anchor_text"))) {
								throw new Exception("dbpedia:anchor_text not allowed in WIMU");
							}
							wURI.setcDatatypesT(mTarget.values().iterator().next().intValue());
							md5 = datasetT.substring(34, datasetT.indexOf("?"));
							wURI.setDatasetT(HDTQueryMan.md5Names.get(md5));
							wURI.setHdtT(datasetT);
						} catch (Exception ex) {
							wURI.setDatasetT(datasetT);
						}
						ret.add(wURI);
					} catch (IOException | ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};
			if (fLink.getName().endsWith(".tql")) {
				RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.NQUADS);
			} else if (fLink.getName().endsWith(".ttl")) {
				RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.TTL);
			} else if (fLink.getName().endsWith(".nt")) {
				RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.NT);
			} else if (fLink.getName().endsWith(".nq")) {
				RDFDataMgr.parse(reader, fLink.getAbsolutePath(), Lang.NQ);
			} else {
				RDFDataMgr.parse(reader, fLink.getAbsolutePath());
			}
			fLink.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return ret;
	}

	public static String set2Json(Set<WIMUri> setWUri) {
		JsonObject json = new JsonObject();
		JsonObject tempj = new JsonObject();
		JsonArray jArr = new JsonArray();
		try {
			for (WIMUri wURI : setWUri) {
				if (wURI != null) {
					tempj = new JsonObject();
					tempj.put("uriS", wURI.getUriS());
					tempj.put("datasetS", wURI.getDatasetS());
					tempj.put("hdtS", wURI.getHdtS());
					tempj.put("cDatatypesS", wURI.getcDatatypesS());
					tempj.put("uriT", wURI.getUriT());
					tempj.put("datasetT", wURI.getDatasetT());
					tempj.put("hdtT", wURI.getHdtT());
					tempj.put("cDatatypesT", wURI.getcDatatypesT());
					jArr.add(tempj);
				}
			}
			json.put("root", jArr);
		} catch (JsonException e) {
			e.printStackTrace();
		}
		return json.toString();
	}

	public static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order, int top) {
		List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		int count = 0;
		// Maintaining insertion order with the help of LinkedList
		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		if (top > 0) {
			for (Entry<String, Integer> entry : list) {
				if (count > (top - 1))
					break;
				sortedMap.put(entry.getKey(), entry.getValue());
				++count;
			}
		} else {
			for (Entry<String, Integer> entry : list) {
				sortedMap.put(entry.getKey(), entry.getValue());
			}
		}
		return sortedMap;
	}
}
