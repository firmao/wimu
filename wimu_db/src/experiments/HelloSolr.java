import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class HelloSolr {

	public static void main(String[] args) throws SolrServerException, IOException {
		HttpSolrClient solr = initialize();
		ConcurrentHashMap<String, Integer> mDatatypeTriples = loadHashMap();
		
		//insertSolr(mDatatypeTriples, solr);
		searchSolr(solr);
	}

	public static HttpSolrClient initialize() {
		// initialize solr
		String urlString = "http://localhost:8983/solr/bigboxstore";
		HttpSolrClient solr = new HttpSolrClient.Builder(urlString).build();
		solr.setParser(new XMLResponseParser());
		return solr;
	}
	
	private static void insertSolr(ConcurrentHashMap<String, Integer> mDatatypeTriples2, HttpSolrClient solr) throws IOException, SolrServerException {
		solr.deleteByQuery("*:*");
		solr.commit();
		mDatatypeTriples2.forEach((uriDs, dTypes) -> {
			String s[] = uriDs.split("\t");
			String uri = s[0];
			String dataset = s[1];
			try {
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField("uri", uri);
				doc.addField("dataset", dataset);
				doc.addField("dtypes", dTypes);
				solr.add(doc);
				solr.commit();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		});
		// iwriter.close();
	}

	public static void searchSolr(HttpSolrClient solr) throws SolrServerException, IOException {
		String uri = "http://dbpedia.org/resource/leipzig";
		SolrQuery query = new SolrQuery();
		query.set("q", "uri:\""+uri+"\"");
		QueryResponse response = solr.query(query);
		 
		SolrDocumentList docList = response.getResults();
		if(docList.getNumFound() > 0){
			for (SolrDocument doc : docList) {
				System.out.println(doc.getFieldValue("dataset"));
				System.out.println(doc.getFieldValue("dtypes"));
			}
		}
	}
	
	private static ConcurrentHashMap<String, Integer> loadHashMap() {
		ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
		
		mDatatypeTriples.put("aa\tds1", 1);
		mDatatypeTriples.put("aa\tds2", 2);
		mDatatypeTriples.put("http://dbpedia.org/resource/Leipzig\tds3", 1);
		mDatatypeTriples.put("aa\tds4", 1);
		mDatatypeTriples.put("aa\tds5", 1);
		mDatatypeTriples.put("aa\tds6", 1);
		mDatatypeTriples.put("http://dbpedia.org/resource/Leipzig1\tds7", 1);
		mDatatypeTriples.put("aa\tds8", 1);
		return mDatatypeTriples;
	}
}
