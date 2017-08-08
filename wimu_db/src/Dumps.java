import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Scanner;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

public class Dumps {

	public static void main(String args[]) throws IOException {
		readBz2File();
	}

	public static void readBz2File() throws IOException {
		URL url = new URL("http://downloads.dbpedia.org/2016-10/core-i18n/en/labels_en.ttl.bz2");
		Scanner scanner = new Scanner(
				new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(url.openStream()))));
		String triple;
		try {
			while ((triple = scanner.nextLine()) != null) {
				// Treat your triples here :)
				System.out.println(triple);
			}
		} finally {
			scanner.close();
		}
	}

	public static File downloadFile(URL url, String path) throws IOException {
		File ret = new File(path);
		System.out.println("opening connection");
		InputStream in = url.openStream();
		FileOutputStream fos = new FileOutputStream(ret);

		System.out.println("reading file...");
		int length = -1;
		byte[] buffer = new byte[1024];// buffer for portion of data from
		// connection
		while ((length = in.read(buffer)) > -1) {
			fos.write(buffer, 0, length);
		}

		fos.close();
		in.close();
		System.out.println("file was downloaded from:");
		System.out.println(url.toString());
		return ret;
	}

}
