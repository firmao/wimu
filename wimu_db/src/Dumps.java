import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.SocketException;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.jena.atlas.lib.Tuple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.tukaani.xz.XZInputStream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

public class Dumps {

	public static Set<String> alreadyProcessed = new HashSet<String>();
	public static Set<String> errorFiles = new HashSet<String>();
	public static Set<String> successFiles = new HashSet<String>();
	public static Set<String> setFileURLs = new HashSet<String>();
	public static Map<String, String> datasetErrorsJena = new ConcurrentHashMap<String, String>();
	public static Map<String, String> mDataTypes = new ConcurrentHashMap<String, String>();
	public static long count = 0;
	public static long totalTriples = 0;
	public static long countDataType = 0;
	public static long lim = 0;
	public static long origLim = 0;
	public static long start = 0;
	public static long totalTime = 0;
	public static boolean dbIndex = false;

	public static void executeDumps(long pLim, boolean pDBindex) throws IOException {
		start = System.currentTimeMillis();
		
		lim = pLim;
		origLim = lim;
		dbIndex = pDBindex;
		System.out.println("LIM = " + lim);
		
		
		int cores = Runtime.getRuntime().availableProcessors();
		List<String> lstURLDumps = FileUtils.readLines(new File("dumpsLocation.txt"), "UTF-8");
		Set<String> setAllFileURLs = getFileURLs(lstURLDumps);
		setFileURLs.addAll(setAllFileURLs);
		// setAllFileServer.forEach(fileName -> {
		for (String fileURL : setAllFileURLs) {
			if (alreadyProcessed.contains(fileURL))
				continue;

			/*
			 * get(download) only files that are not processed yet according to
			 * the number of cores.
			 */
			Set<FileWIMU> setFiles = null;
			try {
				setFileURLs.removeAll(alreadyProcessed);
				
				System.out.println("Starting downloading " + cores + " Files. Already processed files: " 
				+ alreadyProcessed.size() + " from " + setAllFileURLs.size());
				setFiles = getFiles(cores, setFileURLs);
			} catch (IOException e) {
				errorFiles.add(fileURL);
				e.printStackTrace();
			}

			// One file for each processor thread.
			System.out.println("Starting to process " + cores + " threads/files (PARALLEL). Already processed files: " 
				+ alreadyProcessed.size() + " from " + setAllFileURLs.size());
			
			setFiles.parallelStream().forEach(file -> {
			//for (FileWIMU file : setFiles) {
				String provenance = file.getDataset();
				if (processCompressedFile(file, provenance)) {
					successFiles.add(provenance);
					System.out.println("SUCESS: " + provenance);
				} else {
					System.out.println("FAIL: " + provenance + " ERROR: " + datasetErrorsJena.get(provenance));
				}
				alreadyProcessed.add(provenance);
			//}
			});
		}
		String errors = "";
		try{
			System.out.println("Inserting remaining "+ mDataTypes.size() +" Datatypes");
			DBUtil.insert(mDataTypes);
			mDataTypes.clear();
			errors += "Total triples: " + totalTriples + "\nDataTypeTriples: " + countDataType + "\n";
		}catch(Exception ex){
			ex.printStackTrace();
		}
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total Time(ms): " + totalTime);
		errors += "Files processed: " + setAllFileURLs.size() + "\n Total Time: " + totalTime + ""
				+ "\nNumberErrorFiles: " + datasetErrorsJena.size() + "\nNumberFilesSuccess: " + successFiles.size();
		Email.sendEmail("firmao@gmail.com", "End pro DBindex process", errors);
		generateFile(datasetErrorsJena, "ErrorsJena.csv");
	}
	
	public static void main(String args[]) throws IOException {
		start = System.currentTimeMillis();
		
		if(args.length > 0){
			lim = Long.parseLong(args[0]);
			origLim = lim;
			if(args[1].length() > 0){
				dbIndex = Boolean.parseBoolean(args[1]);
			}
		}
		System.out.println("LIM = " + lim);
		
		
		int cores = Runtime.getRuntime().availableProcessors();
		List<String> lstURLDumps = FileUtils.readLines(new File("dumpsLocation.txt"), "UTF-8");
		Set<String> setAllFileURLs = getFileURLs(lstURLDumps);
		setFileURLs.addAll(setAllFileURLs);
		// setAllFileServer.forEach(fileName -> {
		for (String fileURL : setAllFileURLs) {
			if (alreadyProcessed.contains(fileURL))
				continue;

			/*
			 * get(download) only files that are not processed yet according to
			 * the number of cores.
			 */
			Set<FileWIMU> setFiles = null;
			try {
				setFileURLs.removeAll(alreadyProcessed);
				
				System.out.println("Starting downloading " + cores + " Files. Already processed files: " 
				+ alreadyProcessed.size() + " from " + setAllFileURLs.size());
				setFiles = getFiles(cores, setFileURLs);
			} catch (IOException e) {
				errorFiles.add(fileURL);
				e.printStackTrace();
			}

			// One file for each processor thread.
			System.out.println("Starting to process " + cores + " threads/files (PARALLEL). Already processed files: " 
				+ alreadyProcessed.size() + " from " + setAllFileURLs.size());
			
			setFiles.parallelStream().forEach(file -> {
			//for (FileWIMU file : setFiles) {
				String provenance = file.getDataset();
				if (processCompressedFile(file, provenance)) {
					successFiles.add(provenance);
					System.out.println("SUCESS: " + provenance);
				} else {
					System.out.println("FAIL: " + provenance + " ERROR: " + datasetErrorsJena.get(provenance));
				}
				alreadyProcessed.add(provenance);
			//}
			});
		}
		String errors = "";
		try{
			System.out.println("Inserting remaining "+ mDataTypes.size() +" Datatypes");
			DBUtil.insert(mDataTypes);
			mDataTypes.clear();
			errors += "Total triples: " + totalTriples + "\nDataTypeTriples: " + countDataType + "\n";
		}catch(Exception ex){
			ex.printStackTrace();
		}
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total Time(ms): " + totalTime);
		errors += "Files processed: " + setAllFileURLs.size() + "\n Total Time: " + totalTime + ""
				+ "\nNumberErrorFiles: " + datasetErrorsJena.size() + "\nNumberFilesSuccess: " + successFiles.size();
		Email.sendEmail("firmao@gmail.com", "End pro DBindex process", errors);
		generateFile(datasetErrorsJena, "ErrorsJena.csv");
	}

	private static boolean processCompressedFile(FileWIMU file, String dataset) {
		boolean ret = false;
		try {
			FileWIMU fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new FileWIMU(file.getName().replaceAll(".bz2", ""));
			else if (file.getName().endsWith(".xz"))
				fUnzip = new FileWIMU(file.getName().replaceAll(".xz", ""));
			else
				return processUnzipRDF(file);
				
			fUnzip.setDataset(file.getDataset());
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
				ret = processUnzipRDF(fUnzip);
		} catch (Exception ex) {
			ret = false;
			ex.printStackTrace();
		}
		return ret;
	}

	private static boolean processUnzipRDF(FileWIMU fUnzip) {
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
						countDataType++;
						
//						 System.out.println("<" + triple.getSubject().getURI()
//						 + "> <" + triple.getPredicate().getURI()
//						 + "> " + triple.getObject().toString() + " .");
						if(dbIndex){
							mDataTypes.put(triple.getSubject().getURI(), fUnzip.getDataset());
//							try {
//								DBUtil.insert(triple.getSubject().getURI(), fUnzip.getDataset());
//							} catch (ClassNotFoundException e) {
//								e.printStackTrace();
//							} catch (SQLException e) {
//								e.printStackTrace();
//							}
						}
					}
					totalTriples++;
					//System.out.println(count2);
					if(totalTriples > lim){
						if(mDataTypes.size() > 0){
							System.out.println("COMMIT each " + origLim + " Triples");
							try{
								DBUtil.insert(mDataTypes);
								mDataTypes.clear();
							}catch(Exception ex){
								ex.printStackTrace();
							}
						}
						lim += totalTriples;
						System.out.println("new LIM = " + lim);
						//DBUtil.setAutoCommit(true);
//						System.out.println(" - FORCED FINISH !!!");
//						totalTime = System.currentTimeMillis() - start;
//						System.out.println("Total Time(ms): " + totalTime);
//						System.out.println("triples: " + totalTriples);
//						System.out.println("dataTypes: " + countDataType);
//						System.out.println("DBIndex: " + dbIndex);
//						System.out.println("HashMap size:" + mTest.size());
//						fUnzip.delete();
//						System.out.println("Generating file with dataTypes...");
//						generateFile(mTest, "dataTypes.csv");
//						System.out.println("File Generated.");
//						System.exit(0);
					}
				}

				@Override
				public void tuple(Tuple<Node> arg0) {
					// TODO Auto-generated method stub

				}

			};
			if (fUnzip.getName().endsWith(".tql")) {
				// removeFirstLine(fUnzip.getName());
				// removeLastLine(fUnzip.getName());
				RDFDataMgr.parse(reader, fUnzip.getName(), Lang.NQUADS);
			} else if (fUnzip.getName().endsWith(".ttl")) {
				RDFDataMgr.parse(reader, fUnzip.getName(), Lang.TTL);
			} else {
				RDFDataMgr.parse(reader, fUnzip.getName());
			}
			fUnzip.delete();
		} catch (Exception ex) {
			datasetErrorsJena.put(fUnzip.getDataset(), ex.getMessage());
			ret = false;
		}
		return ret;
	}

	private static Set<FileWIMU> getFiles(int cores, Set<String> setFileServer) throws IOException {
		Set<FileWIMU> ret = new HashSet<FileWIMU>();
		count = 0;
		// setAllFileNames.removeAll(alreadyProcessed);
		setFileServer.parallelStream().limit(cores).forEach(sFileURL -> {
			// for (String sFile : setAllFileServer) {
			try {
				String sFileName = getURLFileName(sFileURL);
				FileWIMU fRet = new FileWIMU(sFileName);
				URL url = new URL(sFileURL);
				FileUtils.copyURLToFile(url, fRet);
				fRet.setDataset(sFileURL);
				ret.add(fRet);
				count++;
				System.out.println(count + " : " + sFileURL);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			// }
		});
		return ret;
	}

	private static Set<String> getFileURLs(List<String> pURLs) throws SocketException, IOException {
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
						// ret.add("ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/rdf/"
						// + ftpFile.getName());
						String fName = ftpFile.getName();
						//if (fName.endsWith(".xz"))
						if(filterFileType(fName))
							ret.add(sURL + fName);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else {
				// String url =
				// "http://downloads.dbpedia.org/2016-10/core-i18n/en/";
				try {
					Document doc = Jsoup.connect(sURL).get();
					for (Element file : doc.select("a")) {
						String fName = file.attr("href");
						//if (fName.endsWith(".bz2"))
						if(filterFileType(fName))
							ret.add(sURL + fName);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		});

		return ret;
	}

	private static boolean filterFileType(String fName) {
		boolean ret = false;
			if(fName.endsWith(".ttl.bz2") || fName.endsWith(".tql.bz2")
					|| fName.endsWith("rdf.xz") || fName.endsWith(".rdf")
					|| fName.endsWith(".ttl") || fName.endsWith(".tql") 
					|| fName.endsWith(".nquad"))
				ret = true;
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

				//pool.submit(new MyTask(line));
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
		private FileWIMU file;

		public MyTask(FileWIMU pFile) {
			this.file = pFile;

		}

		public void run() {
			//process file
			String provenance = file.getDataset();
			if (processCompressedFile(file, provenance)) {
				successFiles.add(provenance);
				System.out.println("SUCESS: " + provenance);
			} else {
				System.out.println("FAIL: " + provenance + " ERROR: " + datasetErrorsJena.get(provenance));
			}
			alreadyProcessed.add(provenance);
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

	public static void removeFirstLine(String fileName) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
		// Initial write position
		long writePosition = raf.getFilePointer();
		raf.readLine();
		// Shift the next lines upwards.
		long readPosition = raf.getFilePointer();

		byte[] buff = new byte[1024];
		int n;
		while (-1 != (n = raf.read(buff))) {
			raf.seek(writePosition);
			raf.write(buff, 0, n);
			readPosition += n;
			writePosition += n;
			raf.seek(readPosition);
		}
		raf.setLength(writePosition);
		raf.close();
	}

	public static void removeLastLine(String fileName) throws IOException {
		RandomAccessFile f = new RandomAccessFile(fileName, "rw");
		long length = f.length() - 1;
		byte b;
		do {
			length -= 1;
			f.seek(length);
			b = f.readByte();
		} while (b != 10);
		f.setLength(length + 1);
		f.close();
	}

	public static File generateFile(Map<String, String> endPointErrors, String fileName) {
		File ret = new File(fileName);
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			endPointErrors.forEach((endPoint, error) -> {
				writer.println(endPoint + "\t" + error);
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}
}
