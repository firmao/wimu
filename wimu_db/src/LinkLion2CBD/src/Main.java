import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

import com.google.gson.Gson;

public class Main {

	public static void main(String[] args) throws Exception {
		Set<WIMUri> ret = readJson();
		System.out.println("ret.size: " + ret.size());
		PrintWriter writerS = new PrintWriter("cbdS.nt", "UTF-8");
		PrintWriter writerT = new PrintWriter("cbdT.nt", "UTF-8");
		for (WIMUri wUri : ret) {
			//System.out.println(wUri);
			Map<Set<String>, Set<String>> mCBD_s_t = getAllCBD(wUri);
			mCBD_s_t.entrySet().forEach(entry ->{
				Set<String> cbdS = entry.getKey();
				Set<String> cbdT = entry.getValue();
				cbdS.forEach(tripleS ->{
					writerS.println(tripleS);
				});
				cbdT.forEach(tripleT ->{
					writerT.println(tripleT);
				});
			});
		}
		writerS.close();
		writerT.close();
	}

	private static Map<Set<String>, Set<String>> getAllCBD(WIMUri wUri) throws Exception {
		Map<Set<String>, Set<String>> ret = new HashMap<Set<String>, Set<String>>();
		Set<String> cbdS = getCBD(wUri.getUriS(),wUri.getDatasetS(),wUri.getHdtS());
		Set<String> cbdT = getCBD(wUri.getUriT(),wUri.getDatasetT(),wUri.getHdtT());
		ret.put(cbdS, cbdT);
		return ret;
	}

	private static Set<String> getCBD(String uri, String urlDataset, String urlHDT) throws Exception {
		Set<String> cbd = getCBDHDT(uri,urlHDT);
		if(cbd.size() < 1){
			cbd = getCBDDataset(uri,urlDataset);
		}
		return cbd;
	}

	private static Set<String> getCBDDataset(String uri, String urlDataset) throws Exception {
		Set<String> ret = new HashSet<String>();
		throw new Exception("NEED TO IMPLEMENT");
	}

	private static Set<String> getCBDHDT(String uri, String urlHDT) throws IOException {
		Set<String> ret = new HashSet<String>();
		File file = new File("file.hdt");
		HDT hdt = null;
		try {
			URL url = new URL(urlHDT);
			FileUtils.copyURLToFile(url, file);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			String sparql = "Select * where {?s ?p ?o . filter(?s=<"+uri+"> || ?o=<"+uri+">) }";

			Query query = QueryFactory.create(sparql);

			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			while (results.hasNext()) {
				QuerySolution thisRow = results.next();
				//uriDataset = thisRow.get("s").toString() + "\t" + file.getDataset();
				String nTriple = "<" + thisRow.get("s").toString() + "> <" + thisRow.get("p").toString() + ">";
				if(thisRow.get("o").isLiteral()){
					nTriple += thisRow.get("o").asLiteral().toString() + ". ";
				}else{
					nTriple += " <" + thisRow.get("o").toString() + "> .";
				}
				ret.add(nTriple);
			}
			qe.close();
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			file.delete();
			if(hdt != null){
				hdt.close();
			}
		}	
			
		return ret;
	}

	private static  Set<WIMUri> readJson() throws IOException {
		URL url = new URL("http://139.18.8.58:8080/LinkLion2_WServ/Find?link=http://www.linklion.org/download/mapping/sws.geonames.org---purl.org.nt");
        InputStreamReader reader = new InputStreamReader(url.openStream());
        WIMUri[] dto = new Gson().fromJson(reader, WIMUri[].class);
        Set<WIMUri> ret = Arrays.stream(dto).collect(Collectors.toCollection(HashSet::new));
        return ret;
	}
}
