import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.SocketException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.tukaani.xz.XZInputStream;

public class Dump2DBsub {

	public static Set<String> alreadyProcessed = new HashSet<String>();
	public static Set<String> errorFiles = new HashSet<String>();
	public static Set<String> successFiles = new HashSet<String>();
	public static Set<String> setFileURLs = new HashSet<String>();
	public static ConcurrentHashMap<String, String> datasetErrorsJena = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, Integer> mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
	public static Set<String> setDataTypes = new HashSet<String>();
	public static PrintWriter sWriter = null;
	public static long count = 0;
	public static long countDataTypeTriples = 0;
	public static long totalTriples = 0;
	public static long countFile = 0;
    public static long lim = 500000000;
	//public static long lim = 20000000;
	public static long origLim = 0;
	public static long start = 0;
	public static long totalTime = 0;
	private static int tableIndex;
	public static String fDatypeName = null;
	public static Dataset datasetJena = null;
	private static Connection dbConnection;
	private static final String outDir = "out";
	private static final String dumpDir = "dumpnt";
	private static final String unzipDir = "unzip";

	public static void main(String args[]) throws IOException, ClassNotFoundException, SQLException {

		LogCtl.setLog4j("log4j.properties");
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		File f = new File(outDir);
		if (!f.exists())
			f.mkdir();

		File f1 = new File(dumpDir);
		if (!f1.exists())
			f1.mkdir();

		File f2 = new File(unzipDir);
		if (!f2.exists())
			f2.mkdir();

		execMain(args);
	}

	public static void execMain(String args[]) throws IOException, ClassNotFoundException, SQLException {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println("Dump2DB...Parallel, starting: " + dateFormat.format(date));
		start = System.currentTimeMillis();

		Class.forName("com.mysql.jdbc.Driver");
		dbConnection = DriverManager.getConnection("jdbc:mysql://127.0.0.1/linklion2?" + "user=root&password=sameas");

		// createTable("tmpUriDatatype");

		int cores = Runtime.getRuntime().availableProcessors();
		List<String> lstURLDumps = FileUtils.readLines(new File("dumpsLocation.txt"), "UTF-8");
		Set<String> setAllFileURLs = getFileURLs(lstURLDumps, true);

		if (args.length == 1)
			System.out.println("only Dumps from: " + lstURLDumps);
		else
			setAllFileURLs.addAll(getDumps("lodStatsDatasets.txt"));

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
				long tDownload = System.currentTimeMillis();
				setFileURLs.removeAll(alreadyProcessed);
				System.out.println("Starting downloading " + cores + " Files. Already processed files: "
						+ alreadyProcessed.size() + " from " + setAllFileURLs.size());
				setFiles = getFiles(cores, setFileURLs);
				long totalTDownload = System.currentTimeMillis() - tDownload;
				System.out.println("Total TimeDownload(ms): " + totalTDownload);
			} catch (IOException e) {
				errorFiles.add(fileURL);
				e.printStackTrace();
			}
			System.out.println("Starting to process " + cores + " threads/files. Already processed files: "
					+ alreadyProcessed.size() + " from " + setAllFileURLs.size());

			long tProcess = System.currentTimeMillis();
			System.out.println("Parallel processing files: each lim=" + lim + " dataTypeTriples");
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
			long totalTProcess = System.currentTimeMillis() - tProcess;
			System.out.println("Total TimeProcess(" + cores + " files): " + totalTProcess);
			System.out.println("mDatatypes.size: " + mDatatypeTriples.size());
		}
		System.out.println("Inserting remaining " + mDatatypeTriples.size() + " Datatypes");
		String fileName = "dumps_" + countDataTypeTriples + ".nt";
		insertDB(mDatatypeTriples);

		System.out.println(fileName);
		generateFile(datasetErrorsJena, "ErrorsJena.csv", true);
		totalTime = System.currentTimeMillis() - start;
		System.out.println("Total Time(ms): " + totalTime);
		System.out.println("Dump2DB...Parallel, finalize at: " + dateFormat.format(date));
		System.out.println("countDataTypeTriples: " + countDataTypeTriples);
		//joinTables();
	}

	private static void joinTables() throws SQLException, ClassNotFoundException {
		System.out.println("Joining tables...TODO !");
		CallableStatement callableStatement = null;

		String insertStoreProc = "{call joinTables()}";
		try {
			callableStatement = dbConnection.prepareCall(insertStoreProc);
			callableStatement.executeUpdate();
		} catch (SQLException e) {
			System.err.println(e.getMessage());
		} finally {
			if (callableStatement != null) {
				callableStatement.close();
			}

		}
	}

	private static void insertDB(ConcurrentHashMap<String, Integer> mDatatypeTriples) throws SQLException {
		System.out.println("Insert DB...mDataTypeTriples.size:" + mDatatypeTriples.size());
		long startTime = System.currentTimeMillis();
		PreparedStatement postStatement = dbConnection.prepareStatement(
				"INSERT ignore INTO uriDatatype (uri, dtypes, dataset) " + "VALUES (?, ?, ?)");
		System.out.println("Insert DB...mDataTypeTriples.size:" + mDatatypeTriples.size());
		
		mDatatypeTriples.forEach((uriDs, dTypes) -> {
			String s[] = uriDs.split("\t");
			String uri = s[0];
			String dataset = s[1];
			try {
				postStatement.setString(1, uri);
				postStatement.setInt(2, dTypes);
				postStatement.setString(3, dataset);
				postStatement.addBatch();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		});

		int[] updateCounts = postStatement.executeBatch();
		System.out.println("updateCounts: " + updateCounts.length);
		long totalT = System.currentTimeMillis() - startTime;
		System.out.println("Insert DB, totalTime: " + totalT);
	}

	private static void insertDB(String uri, Integer dTypes, String dataset)
			throws SQLException, ClassNotFoundException {
		// call ADD_DB_U(uri,dataset,dTypes)

		CallableStatement callableStatement = null;

		String insertStoreProc = "{call ADD_DB_U(?,?,?)}";

		try {
			callableStatement = dbConnection.prepareCall(insertStoreProc);

			callableStatement.setString(1, uri);
			callableStatement.setString(2, dataset);
			callableStatement.setInt(3, dTypes);

			callableStatement.executeUpdate();

		} catch (SQLException e) {

			System.err.println(e.getMessage());

		} finally {

			if (callableStatement != null) {
				callableStatement.close();
			}

		}
	}

	private static String createTable() throws SQLException, ClassNotFoundException {

		String tableName = "uriDatatype_" + tableIndex++;

		dbConnection.setAutoCommit(false);
		PreparedStatement prep = dbConnection.prepareStatement("CREATE TABLE " + tableName
				+ " ( uri varchar(2000) NOT NULL, dtypes int(11) DEFAULT NULL, dataset varchar(1000) NOT NULL, PRIMARY KEY (uri,dataset));");
		try {
			prep.executeUpdate();
			dbConnection.commit();
			dbConnection.setAutoCommit(true);
		} catch (SQLException e) {

			System.err.println(e.getMessage());

		} finally {

			if (prep != null) {
				prep.close();
			}

		}
		System.out.println("Created table: " + tableName);
		return tableName;
	}

	private static Set<String> getDumps(String fileName) throws IOException {
		Set<String> setDumps = Files.lines(Paths.get(fileName)).collect(Collectors.toSet());
		Set<String> ret = new HashSet<String>();
		for (String dump : setDumps) {
			if ((dump.length() > 10) && (!dump.contains("sparql")) && (!dump.endsWith("/"))) {
				ret.add(dump);
			}
		}
		return ret;
	}

	private static boolean processCompressedFile(FileWIMU file, String dataset) {
		boolean ret = false;
		try {
			FileWIMU fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new FileWIMU(unzipDir + "/" + file.getName().replaceAll(".bz2", ""));
			else if (file.getName().endsWith(".xz"))
				fUnzip = new FileWIMU(unzipDir + "/" + file.getName().replaceAll(".xz", ""));
			else
				return processUnzipRDF(file);

			fUnzip.setDataset(file.getDataset());

			// if ((fUnzip !=null) && (fUnzip.exists()) && (fUnzip.length() >
			// 1)){
			// System.out.println(fUnzip.getAbsolutePath() + " is already
			// unziped");
			// file.delete();
			// return processUnzipRDF(fUnzip);
			// }
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
			} else {
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
			}

			// file.delete();

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
						//if (reachMaxMemory()) {
							long startTime = System.currentTimeMillis();
							try {
								String tbName = createTable();
								// String tbName = "uriDatatype";
								System.out.println("Reach the limit: " + lim
										+ " Creating another table: " + tbName);
								PreparedStatement postStatement = dbConnection.prepareStatement(
										"INSERT ignore INTO " + tbName + " (uri, dtypes, dataset) " + "VALUES (?, ?, ?)");
								System.out.println("Insert DB...mDataTypeTriples.size:" + mDatatypeTriples.size());
								
								mDatatypeTriples.forEach((uriDs, dTypes) -> {
									String s[] = uriDs.split("\t");
									String uri = s[0];
									String dataset = s[1];
									try {
										postStatement.setString(1, uri);
										postStatement.setInt(2, dTypes);
										postStatement.setString(3, dataset);
										postStatement.addBatch();
									} catch (Exception ex) {
										ex.printStackTrace();
									}
								});

								int[] updateCounts = postStatement.executeBatch();
								System.out.println("updateCounts: " + updateCounts.length);
							} catch (Exception e) {
								e.printStackTrace();
								// System.exit(0);
							}
							long totalT = System.currentTimeMillis() - startTime;
							System.out.println("Insert DB, totalTime: " + totalT);
							System.out.println("countDataTypeTriples: " + countDataTypeTriples);
							System.out.println("totalTriples: " + totalTriples);

							mDatatypeTriples.clear();
							mDatatypeTriples = new ConcurrentHashMap<String, Integer>();
						}

						countDataTypeTriples++;
					}
				}

				private boolean reachMaxMemory() {
					boolean ret = false;
					// Mimimum acceptable free memory you think your app needs
					// (1MB)
					long minRunningMemory = (1024 * 1024);

					Runtime runtime = Runtime.getRuntime();

					if (runtime.freeMemory() < minRunningMemory) {
						System.gc();
						ret = true;
					}
					return ret;
				}
			};
			if (fUnzip.getName().endsWith(".tql")) {
				RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.NQUADS);
			} else if (fUnzip.getName().endsWith(".ttl")) {
				RDFDataMgr.parse(reader, fUnzip.getAbsolutePath(), Lang.TTL);
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
		// for (String sFileURL : setFileServer) {
		System.out.println("Download parallel");
		setFileServer.parallelStream().limit(cores).forEach(sFileURL -> {
			// setFileServer.stream().limit(cores).forEach(sFileURL -> {
			try {
				String sFileName = getURLFileName(sFileURL);
				FileWIMU fRet = new FileWIMU(dumpDir + "/" + sFileName);
				if ((!fRet.exists()) && (fRet.length() < 1)) {
					System.out.println("downloading: " + sFileURL);
					URL url = new URL(sFileURL);
					FileUtils.copyURLToFile(url, fRet);
				}
				fRet.setDataset(sFileURL);
				ret.add(fRet);
				count++;
				System.out.println(count + " : " + sFileURL);
			} catch (Exception ex) {
				datasetErrorsJena.put(sFileURL, ex.getMessage());
				// ex.printStackTrace();
			}
			// }
		});
		return ret;
	}

	private static Set<String> getFileURLs(List<String> pURLs, boolean bSub) throws SocketException, IOException {
		Set<String> ret = new HashSet<String>();

		if(bSub){
			File f = new File(dumpDir);
			ret.addAll(Arrays.asList(f.list()));
			return ret;
		}
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
					Document doc = Jsoup.connect(sURL).get();
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
		File ret = new File(outDir +"/" + fileName);
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
		File ret = new File(outDir +"/" + fileName);
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
		String[] str = pURL.split("/");
		return str[str.length - 1];
	}
}
