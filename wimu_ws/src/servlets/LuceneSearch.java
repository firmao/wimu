package servlets;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

public class LuceneSearch {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

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
//				mResults.forEach((ds, dTypes) -> {
//					System.out.println(ds + "\t" + dTypes);
//				});
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

	public static Map<String, Integer> search(String uri, int maxResults, String luceneDir) throws IOException, ParseException {
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
//		mResults.forEach((ds, dTypes) -> {
//			System.out.println(ds + "\t" + dTypes);
//		});
		if (ireader != null) {
			ireader.close();
		}
		if (directory != null) {
			directory.close();
		}
		return mResults;
	}
}
