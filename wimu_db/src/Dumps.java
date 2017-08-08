import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;

public class Dumps {

	public static void main(String args[]) throws IOException {
		String url = "http://downloads.dbpedia.org/2016-10/core-i18n/en/labels_en.ttl.bz2";
		readBz2File(url);
		// readBz2FileParallel(url);
	}

	public static void readBz2File(String pURL) throws IOException {
		URL url = new URL(pURL);
		Scanner scanner = new Scanner(
				new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(url.openStream()))));
		String triple;
		try {

			int lines = 0;
			// IntStream.range(0,lines).parallel().forEach(action);

			while ((triple = scanner.nextLine()) != null) {
				// Treat your triples here :)
				String[] spo = triple.split("> ");
				if (spo.length > 2) {
					String obj = spo[2];
					
					if(!obj.startsWith("<htt")){
						System.out.println("Object is Literal !!!");
						System.out.println(triple);
						System.out.println("Object: " + obj);
					}
				}
			}
		} finally {
			scanner.close();
		}
	}

	public static void readBz2FileParallel(String pURL) throws IOException {
		URL url = new URL(pURL);
		try {

			// File f = FileUtils.toFile(url);
			File f1 = new File("/home/andre/nada.bz2");
			FileUtils.copyURLToFile(url, f1);
			List<String> lst = FileUtils.readLines(f1, "UTF-8");

			lst.parallelStream().forEach(line -> {
				System.out.println(line);
			});
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
