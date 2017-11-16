import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.query.Dataset;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
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

public class Wimu {
	// Lucene
	public static final Version LUCENE_VERSION = Version.LUCENE_44;
	private static DirectoryReader ireader;
	private static IndexSearcher isearcher;
	private static MMapDirectory directory;

	public static long count = 0;
	public static long countDataTypeTriples = 0;
	public static long totalTriples = 0;
	public static long countFile = 0;
	public static long lim = 10000000;
	// public static long lim = 100000;
	public static long origLim = 0;
	public static long start = 0;
	public static long totalTime = 0;
	public static String fDatypeName = null;
	public static Dataset datasetJena = null;
	private static String luceneDir = "indexLuceneDir";

	public static void main(String args[]) throws IOException, ClassNotFoundException, SQLException, ParseException {

		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		if (args.length > 1) {
			if (args[0].equals("search")) {
				int maxResults = Integer.parseInt(args[2]);
				System.out.println("MaxResults: " + maxResults);
				Set<String> dirs = new HashSet<String>();
				if (args[3] != null) {
					System.out.println("Lucene dirs: " + args[3]);
					String s[] = args[3].split(",");
					for (String sDir : s) {
						dirs.add(sDir);
					}
				} else {
					dirs.add(luceneDir);
					dirs.add("ind1");
					dirs.add("ind2");
				}
				Map<String, Integer> mLucene = luceneSearch(args[1], maxResults, dirs);
				mLucene.forEach((ds, dTypes) -> {
					System.out.println(args[1] + "\t" + ds + "\t" + dTypes);
				});
			} else if (args[0].equals("create")) {
				execMain(args);
			}
		}
	}

	public static Map<String, Integer> luceneSearch(String uri, int maxResults, Set<String> dirs)
			throws IOException, ParseException {
		ConcurrentHashMap<String, Integer> mResults = new ConcurrentHashMap<String, Integer>();
		dirs.parallelStream().forEach(dir -> {
			try {
				System.out.println("Lucene dir: " + dir);
				File indexDirectory = new File(dir);
				// File indexDirectory = new File("indexLuceneDir");
				directory = new MMapDirectory(indexDirectory);
				ireader = DirectoryReader.open(directory);
				isearcher = new IndexSearcher(ireader);
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
				mResults.forEach((ds, dTypes) -> {
					System.out.println(ds + "\t" + dTypes);
				});
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

	public static Map<String, Integer> luceneSearch(String uri, int maxResults) throws IOException, ParseException {
		ConcurrentHashMap<String, Integer> mResults = new ConcurrentHashMap<String, Integer>();
		File indexDirectory = new File(luceneDir);
		directory = new MMapDirectory(indexDirectory);
		System.out.println("Lucene dir: " + indexDirectory.getAbsolutePath());
		ireader = DirectoryReader.open(directory);
		isearcher = new IndexSearcher(ireader);
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
		mResults.forEach((ds, dTypes) -> {
			System.out.println(ds + "\t" + dTypes);
		});
		if (ireader != null) {
			ireader.close();
		}
		if (directory != null) {
			directory.close();
		}
		return mResults;
	}

	public static void execMain(String args[]) throws IOException, ClassNotFoundException, SQLException {
		if (args.length > 3)
			if (args[3].equals("dbpedia")) {
				System.out.println("only Dumps from DBpedia");
				DBpedia.create(args[1], args[2]);
			} else if (args[3].equals("lodstats")) {
				System.out.println("All dumps from LODstats + dbpedia");
				LODStats.create(args[1], args[2]);
			} else {
				System.err.println("Wrong parameters ! " + args[3]);
				System.out.println("search <uri> <num_max_results> <lucenedir_1,lucenedir_n>");
				System.out.println("create <dump_dir> <lucene_name_dir> <dbpedia / lodstats>");
				System.exit(0);
			}
		else {
			System.err.println("Wrong parameters !");
			System.out.println("search <uri> <num_max_results> <lucenedir_1,lucenedir_n>");
			System.out.println("create <dump_dir> <lucene_name_dir> <dbpedia / lodstats>");
			System.exit(0);
		}
	}
}
