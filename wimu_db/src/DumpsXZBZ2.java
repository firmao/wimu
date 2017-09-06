import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.SocketException;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.jena.atlas.lib.Tuple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.tukaani.xz.XZInputStream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

public class DumpsXZBZ2 {

	public static Set<String> alreadyProcessed = new HashSet<String>();
	public static Set<String> errorFiles = new HashSet<String>();

	public static void main(String args[]) throws IOException {
		long start = System.currentTimeMillis();

		int cores = Runtime.getRuntime().availableProcessors();
		List<String> lstURLDumps = FileUtils.readLines(new File("dumpsLocation.txt"), "UTF-8");
		Set<String> setAllFileServer = getFileNames(lstURLDumps);

		// setAllFileServer.forEach(fileName -> {
		for (String fileName : setAllFileServer) {
			String justFName = getURLFileName(fileName);
			if (alreadyProcessed.contains(justFName))
				continue;

			/*
			 * get(download) only files that are not processed yet according to
			 * the number of cores.
			 */
			Set<File> setFiles = null;
			try {
				setFiles = getFiles(cores, setAllFileServer);
			} catch (IOException e) {
				String s[] = fileName.split("/");
				String sFileName = s[s.length - 1];
				errorFiles.add(sFileName);
			}

			// One file for each processor thread.
			setFiles.parallelStream().forEach(file -> {
				if (processCompressedFile(file, fileName)) {
					alreadyProcessed.add(file.getName());
				} else {
					errorFiles.add(file.getName());
				}
			});
			// });
		}
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("Total Time(ms): " + totalTime);
	}

	private static boolean processCompressedFile(File file, String dataset) {
		boolean ret = false;
		try {
			File fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new File(file.getName().replaceAll(".bz2", ""));
			else
				fUnzip = new File(file.getName().replaceAll(".xz", ""));

			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			FileOutputStream out = new FileOutputStream(fUnzip);

			if (file.getName().endsWith(".bz2")) {
				BZip2CompressorInputStream bz2In = new BZip2CompressorInputStream(in);
				final byte[] buffer = new byte[8192];
				int n = 0;
				while (-1 != (n = bz2In.read(buffer))) {
					out.write(buffer, 0, n);
				}
				out.close();
				bz2In.close();
			} else {
				XZInputStream xzIn = new XZInputStream(in);
				final byte[] buffer = new byte[8192];
				int n = 0;
				while (-1 != (n = xzIn.read(buffer))) {
					out.write(buffer, 0, n);
				}
				out.close();
				xzIn.close();
			}

			file.delete();

			if (fUnzip != null)
				ret = processUnzipRDF(fUnzip, dataset);
		} catch (Exception ex) {
			ret = false;
			ex.printStackTrace();
		}
		return ret;
	}

	private static boolean processUnzipRDF(File fUnzip, String dataset) {
		boolean ret = true;
		try {
			StreamRDF reader = new StreamRDF() {

				@Override
				public void base(String arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void finish() {
					// TODO Auto-generated method stub

				}

				@Override
				public void prefix(String arg0, String arg1) {
					// TODO Auto-generated method stub

				}

				@Override
				public void quad(Quad arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void start() {
					// TODO Auto-generated method stub

				}

				@Override
				public void triple(Triple triple) {

					// System.out.println(subject.getURI());
					if (triple.getObject().isLiteral()) {
						// System.out.println("<" + triple.getSubject().getURI()
						// + "> <" + triple.getPredicate().getURI()
						// + "> " + triple.getObject().toString() + " .");

						try {
							DBUtil.insert(triple.getSubject().getURI(), dataset);
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				@Override
				public void tuple(Tuple<Node> arg0) {
					// TODO Auto-generated method stub

				}

			};
			RDFDataMgr.parse(reader, fUnzip.getName());
			fUnzip.delete();
		} catch (Exception ex) {
			ret = false;
		}
		return ret;
	}

	private static Set<File> getFiles(int cores, Set<String> setAllFileServer) throws IOException {
		Set<File> ret = new HashSet<File>();
		int count = 0;
		// setAllFileNames.removeAll(alreadyProcessed);
		for (String sFile : setAllFileServer) {
			String sFileName = getURLFileName(sFile);
			if (alreadyProcessed.contains(sFileName))
				continue;
			if (count > cores)
				break;

			File fRet = new File(sFileName);
			URL url = new URL(sFile);
			FileUtils.copyURLToFile(url, fRet);

			ret.add(fRet);
			count++;
		}

		return ret;
	}

	private static Set<String> getFileNames(List<String> pURLs) throws SocketException, IOException {
		Set<String> ret = new HashSet<String>();

		

		pURLs.forEach(sURL -> {
			if (sURL.startsWith("ftp")) {
				try {
					String s[] = sURL.split("/");
					String domain = s[2];
					String path = sURL.substring(sURL.indexOf(domain) + domain.length());

					FTPClient client = new FTPClient();
					client.connect(domain);
					client.enterLocalPassiveMode();
					client.login("anonymous", "");
					FTPFile[] files = client.listFiles(path);
					for (FTPFile ftpFile : files) {
						//ret.add("ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/rdf/" + ftpFile.getName());
						String fName = ftpFile.getName();
						if(fName.endsWith(".xz"))
							ret.add(sURL + fName);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else {
				//String url = "http://downloads.dbpedia.org/2016-10/core-i18n/en/";
				try {
					Document doc = Jsoup.connect(sURL).get();
					for (Element file : doc.select("a")) {
						String fName = file.attr("href");
						if (fName.endsWith(".bz2"))
							ret.add(fName);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		});

		return ret;
	}

	public static void readHugeFile(String pFileName) throws IOException {

		File f = new File(pFileName);
		// LineIterator it = FileUtils.lineIterator(f, "UTF-16LE");
		ExecutorService pool = Executors.newFixedThreadPool(1000);
		int count = 0;

		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
			inputStream = new FileInputStream(f);
			sc = new Scanner(inputStream, "UTF-8");
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				// System.out.println(line);

				if (count > 150) {
					try {
						Thread.sleep(100);
						count = 0;
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				pool.submit(new MyTask(line));
				count++;
			}
			// note that Scanner suppresses exceptions
			if (sc.ioException() != null) {
				throw sc.ioException();
			}
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (sc != null) {
				sc.close();
			}
		}
	}

	static class MyTask implements Runnable {
		private String lLine;

		public MyTask(String line) {
			this.lLine = line;

		}

		public void run() {
			// System.out.println(lLine);
			String[] triple = lLine.split(",");
			String s = triple[0];
			String o = triple[2];
			if (!o.startsWith("http")) {
				// System.out.println("Literal");
				System.out.println(lLine);
				// setDataType.add(lLine);
			} else {
				System.out.println("<not yet>");
			}
		}
	}

	public static void readBz2File(String pURL) throws IOException {
		int countTotal = 0;
		Set<String> setDataTypes = new HashSet<String>();
		URL url = new URL(pURL);
		Scanner scanner = new Scanner(
				new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(url.openStream()))));
		String triple;
		String fileName;
		try {

			int lines = 0;
			// IntStream.range(0,lines).parallel().forEach(action);

			fileName = getURLFileName(url);
			System.out.println("File: " + fileName);
			while (scanner.hasNextLine()) {
				// while ((triple = scanner.nextLine()) != null) {
				triple = scanner.nextLine();
				String[] spo = triple.split("> ");
				if (spo.length > 2) {
					// String sub = spo[0];
					String obj = spo[2];
					if (!obj.startsWith("<htt")) {
						// System.out.println("Object is Literal !!!");
						// System.out.println(triple);
						// System.out.println("Object: " + obj);
						countTotal++;
						setDataTypes.add(triple);
					}
				}
			}
		} finally {
			scanner.close();
		}
		System.out.println("TotalTriples: " + countTotal);
		System.out.println("Total DataTypes: " + setDataTypes.size());
		generateFile(setDataTypes, fileName + "_dTypes.ttl");
	}

	public static void generateFile(Set<String> endPointErrors, String fileName) {
		try {
			// File f = new File(fileName);
			// if(!f.exists()) f.createNewFile();
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			endPointErrors.forEach(endPoint -> {
				writer.println(endPoint);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String getURLFileName(URL pURL) {
		String[] str = pURL.getFile().split("/");
		return str[str.length - 1];
	}

	public static String getURLFileName(String pURL) {
		String[] str = pURL.split("/");
		return str[str.length - 1];
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
