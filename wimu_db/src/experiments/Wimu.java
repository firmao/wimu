import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.lucene.queryparser.classic.ParseException;

public class Wimu {

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
				}
				Map<String, Integer> mLucene = WimuSearch.luceneSearch(args[1], maxResults, dirs);
				mLucene.forEach((ds, dTypes) -> {
					System.out.println(ds + "\t" + dTypes);
				});
			} else if (args[0].equals("create")) {
				create(args);
			}
		}
	}



	public static void create(String args[]) throws IOException, ClassNotFoundException, SQLException {
		if (args.length > 3)
			if (args[3].equals("dbpedia")) {
				System.out.println("only Dumps from DBpedia");
				DBpedia.create(args[1], args[2]);
			} else if (args[3].equals("lodstats")) {
				System.out.println("All dumps from LODstats + dbpedia");
				LODStats.create(args[1], args[2]);
			}else if (args[3].equals("endpoints")) {
				System.out.println("All EndPoints from LODstats + dbpedia");
				Endpoint2Lucene.create(args[2]); 
			} else {
				System.err.println("Wrong parameters ! " + args[3]);
				System.out.println("search <uri> <num_max_results> <lucenedir_1,lucenedir_n>");
				System.out.println("create <dump_dir> <lucene_name_dir> <dbpedia / lodstats / endpoints>");
				System.exit(0);
			}
		else {
			System.err.println("Wrong parameters !");
			System.out.println("search <uri> <num_max_results> <lucenedir_1,lucenedir_n>");
			System.out.println("create <dump_dir> <lucene_name_dir> <dbpedia / lodstats / endpoints>");
			System.exit(0);
		}
	}
}
