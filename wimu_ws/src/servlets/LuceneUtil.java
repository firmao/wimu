package servlets;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
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
}
