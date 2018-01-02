import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.tukaani.xz.XZInputStream;

public class LODStatsBKP {
	public static Set<String> alreadyProcessed = new HashSet<String>();
	public static Set<String> errorFiles = new HashSet<String>();
	public static Set<String> successFiles = new HashSet<String>();
	public static Set<String> setFileURLs = new HashSet<String>();
	public static ConcurrentHashMap<String, String> datasetErrorsJena = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
	// public static Set<String> setDataTypes = new HashSet<String>();

	// Lucene
	public static final String N_TRIPLES = "NTriples";
	public static final String TTL = "ttl";
	public static final String TSV = "tsv";
	public static final Version LUCENE_VERSION = Version.LUCENE_44;
	private static Analyzer urlAnalyzer;
	private static DirectoryReader ireader;
	private static IndexWriter iwriter;
	private static MMapDirectory directory;

	public static long count = 0;
	public static long countDataTypeTriples = 0;
	public static long totalTriples = 0;
	public static long countFile = 0;
	// public static long lim = 30000000;
	public static long lim = 100000;
	public static long origLim = 0;
	public static long start = 0;
	public static long totalTime = 0;
	public static String fDatypeName = null;
	public static Dataset datasetJena = null;
	private static String dumpDir;
	private static String luceneDir;

	public static void main(String args[]) throws MalformedURLException, IOException {
		create("dumpDir", "luceneDir");
	}

	public static void create(String pDumpDir, String pLuceneDir) throws IOException {

		dumpDir = pDumpDir;
		luceneDir = pLuceneDir;

		File f = new File("out");
		if (!f.exists())
			f.mkdir();

		File f1 = new File(dumpDir);
		if (!f1.exists())
			f1.mkdir();

		File f2 = new File("unzip");
		if (!f2.exists())
			f2.mkdir();

		System.out.println("Dumps dir: " + f1.getAbsolutePath());

		File indexDirectory = new File(luceneDir);
		indexDirectory.mkdir();
		directory = new MMapDirectory(indexDirectory);
		System.out.println("Created Lucene dir: " + indexDirectory.getAbsolutePath());
		urlAnalyzer = new SimpleAnalyzer(LUCENE_VERSION);
		Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
		mapping.put("uri", urlAnalyzer);
		mapping.put("dataset_dtype", urlAnalyzer);
		PerFieldAnalyzerWrapper perFieldAnalyzer = new PerFieldAnalyzerWrapper(urlAnalyzer, mapping);
		IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, perFieldAnalyzer);
		iwriter = new IndexWriter(directory, config);

		// iwriter.deleteAll();
		//
		iwriter.commit();

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("Dump2Lucene...Parallel, starting: " + dateFormat.format(date));
		start = System.currentTimeMillis();

		int cores = Runtime.getRuntime().availableProcessors();
		alreadyProcessed.addAll(WimuUtil.getAlreadyProcessed(Wimu.logFileName));
		// List<String> lstURLDumps = FileUtils.readLines(new
		// File("dumpsLocation.txt"), "UTF-8");
		List<String> lstURLDumps = new ArrayList<String>();
		Set<String> setAllFileURLs = getFileURLs(lstURLDumps);

		setAllFileURLs.addAll(getDumps("lodStatsDatasets.txt"));
		setAllFileURLs.removeAll(alreadyProcessed);

		setFileURLs.addAll(setAllFileURLs);
		// setAllFileServer.forEach(fileName -> {
		for (String fileURL : setAllFileURLs) {
			if (alreadyProcessed.contains(fileURL))
				continue;

			Set<FileWIMU> setFiles = null;
			try {
				long tDownload = System.currentTimeMillis();
				setFileURLs.removeAll(alreadyProcessed);
				System.out.println("Starting downloading " + cores + " Files. Already processed files: "
						+ alreadyProcessed.size() + " from " + setAllFileURLs.size());
				setFiles = getFiles(cores, setFileURLs);
				long totalTDownload = System.currentTimeMillis() - tDownload;
				System.out.println("Total TimeDownload(ms): " + totalTDownload);
			} catch (IOException e) {
				datasetErrorsJena.put(fileURL,e.getMessage());
			}
			System.out.println("Starting to process " + cores + " threads/files. Already processed files: "
					+ alreadyProcessed.size() + " from " + setAllFileURLs.size());

			long tProcess = System.currentTimeMillis();
			System.out.println("Parallel processing##");
			setFiles.parallelStream().forEach(file -> {
				// for (FileWIMU file : setFiles) {
				String provenance = file.getDataset();
				if (processCompressedFile(file, provenance)) {
					synchronized (successFiles) {
						successFiles.add(provenance);
					}
					System.out.println("SUCESS: " + provenance);
				} else {
					System.out.println("FAIL: " + provenance + " ERROR: " + datasetErrorsJena.get(provenance));
				}

				alreadyProcessed.add(provenance);

				// }
			});
			long totalTProcess = System.currentTimeMillis() - tProcess;
			System.out.println("Total TimeProcess(" + cores + " files): " + totalTProcess);
			System.out.println("mDatatypes.size: " + mDatatypeTriples.size());
		}
		System.out.println("Inserting remaining " + mDatatypeTriples.size() + " Datatypes");
		insertLucene(mDatatypeTriples);
		generateFile(datasetErrorsJena, "ErrorsJena.csv", true);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total Time(ms): " + totalTime);
		System.out.println("countDataTypeTriples: " + countDataTypeTriples);
		date = new Date();
		System.out.println("Dump2Lucene...Parallel, finalize at: " + dateFormat.format(date));
		if (iwriter != null) {
			iwriter.close();
		}
		if (ireader != null) {
			ireader.close();
		}
		if (directory != null) {
			directory.close();
		}
	}

	private static void insertLucene(ConcurrentHashMap<String, Integer> mDatatypeTriples2) throws IOException {
		System.out.println("Insert Lucene");
		// mDatatypeTriples2.forEach((uriDs, dTypes) -> {
		mDatatypeTriples.entrySet().parallelStream().forEach(elem -> {
			String uriDs = elem.getKey();
			int dTypes = elem.getValue();

			String s[] = uriDs.split("\t");
			String uri = s[0];
			String dataset = s[1];
			try {
				Document doc = new Document();
				doc.add(new StringField("uri", uri, Store.YES));
				doc.add(new StringField("dataset_dtype", dataset + "\t" + dTypes, Store.YES));
				iwriter.addDocument(doc);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		});
		// iwriter.close();
	}

	private static Set<String> getDumps(String fileName) throws IOException {
		Set<String> setDumps = Files.lines(Paths.get(fileName)).collect(Collectors.toSet());
		Set<String> ret = new HashSet<String>();
		for (String dump : setDumps) {
			if ((dump.length() > 10) && (!dump.contains("sparql")) && (!dump.endsWith("/"))) {
				if (isGoodURL(dump)) {
					ret.add(dump);
				} else {
					System.out.println("FAIL: " + dump + " ERROR: Invalid URI, not possible to download the content.");
					datasetErrorsJena.put(dump, "ERROR: Invalid URI, not possible to download the content.");
				}
			}else{
				System.out.println("FAIL: " + dump + " ERROR: Not a valid dump file.");
				datasetErrorsJena.put(dump, "ERROR: Not a valid dump file.");
			}
		}
		return ret;
	}

	private static boolean isValidExtension(String dump) {
		if(dump.endsWith(".bz2") || dump.endsWith(".xz") 
				|| dump.endsWith(".zip") || dump.endsWith(".tar.gz")
				|| dump.endsWith(".tql") || dump.endsWith(".ttl")
				|| dump.endsWith(".nt") || dump.endsWith(".nq")
				|| dump.endsWith(".rdf") || dump.endsWith(".owl"))
			return true;
		else
			return false;
	}

	private static boolean processCompressedFile(FileWIMU file, String dataset) {
		boolean ret = false;
		try {
			System.out.println("Processing: " + dataset);
			FileWIMU fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new FileWIMU("unzip/" + file.getName().replaceAll(".bz2", ""));
			else if (file.getName().endsWith(".xz"))
				fUnzip = new FileWIMU("unzip/" + file.getName().replaceAll(".xz", ""));
			else if (file.getName().endsWith(".zip"))
				fUnzip = new FileWIMU("unzip/" + file.getName().replaceAll(".zip", ""));
			else if (file.getName().endsWith(".tar.gz"))
				fUnzip = new FileWIMU("unzip/" + file.getName().replaceAll(".tar.gz", ""));
			else
				return processUnzipRDF(file);

			fUnzip.setDataset(file.getDataset());
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			FileOutputStream out = new FileOutputStream(fUnzip);

			if (file.getName().endsWith(".bz2")) {
				BZip2CompressorInputStream bz2In = new BZip2CompressorInputStream(in);
				synchronized (bz2In) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = bz2In.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					bz2In.close();
				}
			} else if (file.getName().endsWith(".xz")) {
				XZInputStream xzIn = new XZInputStream(in);
				synchronized (xzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = xzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					xzIn.close();
				}
			} else if (file.getName().endsWith(".zip")) {
				ZipArchiveInputStream zipIn = new ZipArchiveInputStream(in);
				synchronized (zipIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = zipIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					zipIn.close();
				}
			} else if (file.getName().endsWith(".tar.gz")) {
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
				synchronized (gzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = gzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					gzIn.close();
				}
			}

			file.delete();

			if (fUnzip != null)
				ret = processUnzipRDF(fUnzip);
		} catch (Exception ex) {
			ret = false;
			datasetErrorsJena.put(dataset, ex.getMessage());
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
					totalTriples++;
					if (triple.getObject().isLiteral()) {
						String uriDataset = triple.getSubject().getURI() + "\t" + fUnzip.getDataset();
						if (mDatatypeTriples.containsKey(uriDataset)) {
							int cDtypes = mDatatypeTriples.get(uriDataset) + 1;
							mDatatypeTriples.put(uriDataset, cDtypes);
						} else {
							mDatatypeTriples.put(uriDataset, 1);
						}

						if (mDatatypeTriples.size() > lim) {
							long startTime = System.currentTimeMillis();
							System.out.println("Reach the limit: " + lim + " Inserting to " + luceneDir);
							try {
								insertLucene(mDatatypeTriples);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							long totalT = System.currentTimeMillis() - startTime;
							System.out.println("Insert Lucene " + countDataTypeTriples
									+ " DataTypeTriples, in totalTime: " + totalT);
							System.out.println("totalTriples: " + totalTriples);

							mDatatypeTriples.clear();
							mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
						}
						countDataTypeTriples++;
					}
				}
			};
			if (fUnzip.getName().endsWith(".tql")) {
				RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NQUADS);
			} else if (fUnzip.getName().endsWith(".ttl")) {
				RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.TTL);
			} else if (fUnzip.getName().endsWith(".nt")) {
				RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NT);
			} else if (fUnzip.getName().endsWith(".nq")) {
				RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NQ);
			} else {
				RDFDataMgr.parse(reader, fUnzip.getAbsolutePath());
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
		// setFileServer.parallelStream().limit(cores).forEach(sFileURL -> {
		System.out.println("Download Files_");
		for (String sFileURL : setFileServer) {
			if (count == cores)
				break;
			// setFileServer.parallelStream().limit(cores).forEach(sFileURL -> {
			// setFileServer.stream().limit(cores).forEach(sFileURL -> {
			try {
				System.out.println("Start download: " + sFileURL);
				// if(!isGoodURL(sFileURL)) {
				// alreadyProcessed.add(sFileURL);
				// System.out.println("FAIL: " + sFileURL + " ERROR: Invalid
				// URI, not possible to download the content.");
				// throw new Exception("Invalid URI, not possible to download
				// the content.");
				// }
				String sFileName = getURLFileName(sFileURL);
//				if(!isValidExtension(sFileName)){
//					throw new Exception("Invalid file extension.");
//				}
				FileWIMU fRet = new FileWIMU(dumpDir + "/" + sFileName);
				URL url = new URL(sFileURL);
				FileUtils.copyURLToFile(url, fRet);
				fRet.setDataset(sFileURL);
				ret.add(fRet);
				count++;
				System.out.println(count + " : " + sFileURL + " size(bytes): " + fRet.length());
			} catch (Exception ex) {
				if (datasetErrorsJena.mappingCount() > lim) {
					generateFile(datasetErrorsJena, "ErrorsJena_" + totalTriples + ".csv", true);
					datasetErrorsJena.clear();
					datasetErrorsJena = new ConcurrentHashMap<String, String>();
				}
				datasetErrorsJena.put(sFileURL, ex.getMessage());
				System.out.println("FAIL: " + sFileURL + " ERROR: " + ex.getMessage());
				// ex.printStackTrace();
			}
		}
		// });
		return ret;
	}

	private static boolean isGoodURL(String url) {
		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setRequestMethod("HEAD");
			int responseCode = connection.getResponseCode();
			if ((responseCode == 200) || (responseCode == 400)) {
				return true;
			} else
				return false;
		} catch (Exception e) {
			return false;
		}
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
						// if (fName.endsWith(".xz"))
						if (filterFileType(fName))
							ret.add(sURL + fName);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			} else {
				// String url =
				// "http://downloads.dbpedia.org/2016-10/core-i18n/en/";
				try {
					org.jsoup.nodes.Document doc = Jsoup.connect(sURL).get();
					for (Element file : doc.select("a")) {
						String fName = file.attr("href");
						// if (fName.endsWith(".bz2"))
						if (filterFileType(fName))
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
		if (fName.endsWith(".ttl.bz2") || fName.endsWith(".tql.bz2") || fName.endsWith("rdf.xz")
				|| fName.endsWith(".rdf") || fName.endsWith(".ttl") || fName.endsWith(".tql")
				|| fName.endsWith(".nquad"))
			ret = true;
		return ret;
	}

	public static void generateFile(Map<String, Integer> maps, String fileName) {
		File ret = new File("out/" + fileName);
		System.out.println("Generating file: " + ret.getAbsolutePath());
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			maps.forEach((uriDataset, dTypes) -> {
				writer.println(uriDataset + " \"" + dTypes + "\"^^<http://www.w3.org/2001/XMLSchema#int> .");
			});
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("File generated: " + ret.getAbsolutePath());
	}

	public static File generateFile(Map<String, String> endPointErrors, String fileName, boolean b) {
		File ret = new File("out/" + fileName);
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

	public static String getURLFileName(URL pURL) {
		String[] str = pURL.getFile().split("/");
		return str[str.length - 1];
	}

	public static String getURLFileName(String pURL) {
		String ret = null;
		String[] str = pURL.split("/");
		ret = str[str.length - 1];
		if (ret.indexOf('?') > 0)
			ret = ret.split("\\?")[0];

		File f = new File(dumpDir + "/" + ret);
		if (f.exists())
			ret = ret.replace(".", "_" + (countFile++) + ".");

		return ret;
	}
}
