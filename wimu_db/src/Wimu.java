import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.atlas.logging.LogCtl;
import org.rdfhdt.hdt.exceptions.ParserException;

public class Wimu {
	public static String logFileName = null;
	public static void main(String args[]) throws IOException, ClassNotFoundException, SQLException, ParseException, org.apache.lucene.queryparser.classic.ParseException, ParserException {

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
				System.out.println("URI: " + args[1]);
				long timeStart = System.currentTimeMillis();
				Map<String, Integer> mLucene = WimuSearch.luceneSearch(args[1], maxResults, dirs);
				long totalTime = System.currentTimeMillis() - timeStart;
				System.out.println("Total time search: " + totalTime);
				mLucene.forEach((ds, dTypes) -> {
					System.out.println(ds + "\t" + dTypes);
				});
			} else if (args[0].equals("create")) {
				create(args);
			} else if (args[0].equals("logs")) {
				WimuUtil.analyseLogFiles("logs");
			}
		}
	}



	public static void create(String args[]) throws IOException, ClassNotFoundException, SQLException, ParserException {
		if (args.length > 3){
			logFileName = args[4];
			System.out.println("logFileName: " + logFileName);

			if (args[3].equals("dbpedia")) {
				System.out.println("Dumps from DBpedia");
				DBpedia.create(args[1], args[2]);
			} else if (args[3].equals("dbpedia2hdt")) {
				System.out.println("dumps from DBpedia to HDT");
				DBpedia2HDT.create(args[1], args[2]);
			} else if (args[3].equals("dumps")) {
				System.out.println("All dumps from LODstats");
				LODStats.create(args[1], args[2]);
			} else if (args[3].equals("hdtParallel")) {
				System.out.println("All HDT files from LODLaundromat (Parallel)");
				HDTFilesParallel.create(args[1], args[2]);
			} else if (args[3].equals("rdfhdt.org")) {
				System.out.println("All HDT files from rdfhdt.org (Parallel)");
				RDF_HDT_org.create(args[1], args[2]);
			} else if (args[3].equals("endpoints")) {
				System.out.println("All EndPoints from LODstats");
				Endpoint2Lucene.create(args[2]); 
			} else if (args[3].equals("all")) {
				System.out.println("Everything DBpedia + LODStats + HDTFilesLODLaundromat (Dumps + Endpoints)");
				System.out.println("Dumps from DBpedia");
				DBpedia.create(args[1], args[2]);
				System.out.println("All dumps from LODstats");
				LODStats.create(args[1], args[2]);
				System.out.println("All EndPoints from LODstats");
				Endpoint2Lucene.create(args[2]); 
				System.out.println("All HDT files from LODLaundromat");
				HDTFilesParallel.create(args[1], args[2]);
			} else {
				System.err.println("Wrong parameters ! " + args[3]);
				System.out.println("search <uri> <num_max_results> <lucenedir_1,lucenedir_n>");
				System.out.println("create <dump_dir> <lucene_name_dir> <dbpedia / lodstats / endpoints / all> <logFileName>");
				System.exit(0);
			}
		}else {
			System.err.println("Wrong parameters !");
			System.out.println("search <uri> <num_max_results> <lucenedir_1,lucenedir_n>");
			System.out.println("create <dump_dir> <lucene_name_dir> <dbpedia / lodstats / endpoints / all> <logFileName>");
			System.exit(0);
		}
	}
}
