import java.io.File;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

public class DownloadHDT {

	public static void main(String[] args) {
		Map<String, String> mMD5HDT = getFiles();
		generateFile(mMD5HDT, "md5HDT.csv");
	}

	private static Map<String, String> getFiles() {
		Map<String, String> mResult = new HashMap<String, String>();
		String endPoint = "http://sparql.backend.lodlaundromat.org/";

		final long offsetSize = 9999;
		long offset = 0;
		do {
			String sparqlQueryString = "PREFIX llo: <http://lodlaundromat.org/ontology/> "
					+ "PREFIX ll: <http://lodlaundromat.org/resource/> " + "SELECT ?datadoc ?md5 ?url ?triples "
					+ "WHERE { { ?datadoc llo:url ?url ; llo:triples ?triples ; llo:md5 ?md5 . } "
					+ "UNION { ?datadoc llo:path ?url ; llo:triples ?triples ; llo:md5 ?md5 . } "
					+ "UNION { ?datadoc a llo:Archive ; llo:md5 ?md5; llo:containsEntry []. "
					+ "{?datadoc llo:url ?url} " + "UNION {?datadoc llo:path ?url} } } offset " + offset + " limit "
					+ offsetSize;

			Query query = QueryFactory.create(sparqlQueryString);

			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			try {
				ResultSet results = qexec.execSelect();
				for (; results.hasNext();) {
					try {
						QuerySolution qSol = results.nextSolution();
						String md5 = qSol.get("?md5").toString();
						String url = qSol.get("?url").toString();
						String md5URL = "http://download.lodlaundromat.org/" + md5 + "?type=hdt";
						mResult.put(md5, url);
						download(md5URL, md5);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				break;
			} finally {
				qexec.close();
			}
			System.out.print(offset);
			offset += offsetSize;
		} while (true);

		return mResult;
	}

	private static void download(String md5url, String md5) {
		try {
			File dir = new File("dirHDT");
			dir.mkdir();
			File fRet = new File(dir + "/" + md5 + ".hdt");
			URL url = new URL(md5url);
			FileUtils.copyURLToFile(url, fRet);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static File generateFile(Map<String, String> map, String fileName) {
		File ret = new File(fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			map.forEach((endPoint, error) -> {
				writer.println(endPoint + "\t" + error);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}

}
