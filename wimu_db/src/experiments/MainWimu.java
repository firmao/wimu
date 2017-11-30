import java.io.IOException;
import java.sql.SQLException;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

public class MainWimu {

	public static void main(String args[])
			throws IOException, ClassNotFoundException, SQLException, SolrServerException {

		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		if (args.length > 1) {
			System.out.println(args[0]);
			if (args[0].equals("create")) {
				create(args);
			} else if (args[0].equals("select")) {
				search(args);
			}
		}
	}

	private static void search(String[] args) throws SolrServerException, IOException {
		HttpSolrClient solr = SolrUtil.initialize();
		System.out.println(args[1]);
		if (args[1].equals("*"))
			SolrUtil.selectAll(solr);
		else
			SolrUtil.search(solr, args[1]);
	}

	public static void create(String args[])
			throws IOException, ClassNotFoundException, SQLException, SolrServerException {
		System.out.println(args[1]);
		if (args[1].equals("endpoints")) {
			System.out.println("Creating index (only SPARQL Endpoints from LODStats) - !!! Takes more than 5 days !!!");
			Endpoint2Solr.create();
		} else if (args[1].equals("dumps")) {
			System.out.println("Creating index (only dumps from LODStats) - !!! Takes more than 5 days !!!");
			Dump2Solr.create("dumpsWimu");
		} else if (args[1].equals("dbpedia")) {
			System.out.println("Create index (only DBpedia dumps) - !!! Takes 9 hours !!!");
			DBpedia2Solr.create("dumpsWimu");
		} else if (args[1].equals("dbpediaEndpoint")) {
			System.out.println("Create index (only DBpedia dumps) - !!! Takes 9 hours !!!");
			DBpediaEndpoint2Solr.create();
		} else if (args[1].equals("*")) {
			System.out.println("Create index (Dumps + EndPoints) - !!! Takes more than 7 days !!!");
			Dump2Solr.create("dumpsWimu");
			Endpoint2Solr.create();
		} else {
			System.err.println("Wrong parameters ! " + args[3]);
			System.out.println("search <uri / *>");
			System.out.println("create <dbpedia / dumps / endpoints / dbpediaEndpoint / *>");
			System.exit(0);
		}
	}
}
