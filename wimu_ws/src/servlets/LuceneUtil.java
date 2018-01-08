package servlets;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
		HDTQueryMan.loadFileMap("md5HDT.csv");
		String dirHDT = System.getProperty("user.home") + "/hdtDatasets";
		//String dirHDT = "/media/andre/TOSHIBA\\ EXT/hdtAmazon/hdtFiles/";
		createHDTLuceneIndex("dirLuceneHDT", new File(dirHDT));
	}

	private static void createHDTLuceneIndex(String dirLucene, File dirHDT) throws IOException {
		long start = System.currentTimeMillis();
		createLuceneDir(dirLucene);
		Set<File> datasets = null;
		try {
			datasets = Files.walk(Paths.get(dirHDT.getPath())).filter(Files::isRegularFile).map(Path::toFile)
					.collect(Collectors.toSet());
			datasets.forEach(dataset -> {
			//datasets.parallelStream().forEach(dataset -> {
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
					while(results.hasNext()) {
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

		//iwriter.deleteAll();
		//
		iwriter.commit();
	}

	private static void insertLucene(HashMap<String, Integer> mDatatypeTriples2) throws IOException {
		System.out.println("Inserting Lucene " + mDatatypeTriples2.size() + " dataType triples");
		mDatatypeTriples2.entrySet().parallelStream().forEach(elem -> {
		//mDatatypeTriples2.entrySet().forEach(elem -> {
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

	public static Map<String, Integer> search(String uri, int maxResults, Set<String> dirs)
			throws IOException, ParseException {
		ConcurrentHashMap<String, Integer> mResults = new ConcurrentHashMap<String, Integer>();
		dirs.parallelStream().forEach(dir -> {
			try {
				System.out.println("Lucene dir: " + dir);
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
				e.printStackTrace();
			}
		});
		return mResults;
	}

	public static Map<String, Integer> search(String uri, int maxResults, String luceneDir)
			throws IOException, ParseException {
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
		return mResults;
	}

	public static Set<WIMUri> search(File fLink, int maxResults, Set<String> dirs) {
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
					WIMUri wURI = new WIMUri(source,target);
					try {
						Map<String, Integer> mSource = sortByComparator(search(source, maxResults, dirs),false,1);
						Map<String, Integer> mTarget = sortByComparator(search(target, maxResults, dirs),false,1);
						String datasetS = mSource.keySet().iterator().next(); 
						String datasetT = mSource.keySet().iterator().next();
						String md5 = null;
						try{
							md5 = datasetS.substring(34, datasetS.indexOf("?"));
							wURI.setDatasetS(HDTQueryMan.md5Names.get(md5));
							wURI.setHdtS(datasetS);
						}catch(Exception ex){
							wURI.setDatasetS(datasetS);
						}
						try{
							md5 = datasetT.substring(34, datasetT.indexOf("?"));
							wURI.setDatasetT(HDTQueryMan.md5Names.get(md5));
							wURI.setHdtT(datasetT);
						}catch(Exception ex){
							wURI.setDatasetT(datasetT);
						}
						wURI.setcDatatypesS(mSource.values().iterator().next().intValue());
						wURI.setcDatatypesT(mTarget.values().iterator().next().intValue());
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
	
	public static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order, int top)
    {
        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        int count = 0;
        // Maintaining insertion order with the help of LinkedList
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list)
        {
        	if(count > (top-1)) break;
            sortedMap.put(entry.getKey(), entry.getValue());
            ++count;
        }

        return sortedMap;
    }
}
