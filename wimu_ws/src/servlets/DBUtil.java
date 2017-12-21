package servlets;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DBUtil {

	private static Connection connGeneric;

	// Select name, countDType from dataset2, uri2 where indexDataset=indDS and
	// uri='http://www.w3.org/2000/01/rdf-schema#Literal';
	public static Map<String, Integer> findEndPoint(String uri) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			Connection connection = getSQLConnection();
			ps = connection.prepareStatement(
					"Select name, countDType from dataset2, uri2 where indexDataset=indDS and uri = ?");
			ps.setString(1, uri);
			rs = ps.executeQuery();
			while (rs.next()) {
				String endPoint = rs.getString("name");
				int countDType = rs.getInt("countDType");
				result.put(endPoint, countDType);
			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(DBUtil.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				// if (conn != null) {
				// conn.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(DBUtil.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		Map<String, Integer> mDumps = findDumps2(uri);
		result.putAll(mDumps);
		return result;
	}

	private static Map<String, Integer> findDumps(String uri) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			Connection connection = getSQLConnection();
			ps = connection.prepareStatement("Select count(o) as dTypes, o from datatypes where s = ? group by o");
			ps.setString(1, uri);
			rs = ps.executeQuery();
			while (rs.next()) {
				String endPoint = rs.getString("o");
				int countDType = rs.getInt("dTypes");
				result.put(endPoint, countDType);
			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(DBUtil.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				// if (conn != null) {
				// conn.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(DBUtil.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		return result;
	}

	private static Map<String, Integer> findDumps2(String uri) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			Connection connection = getSQLConnection();
			int limTables = getLimTables();
			String sql = null;
			if(limTables > 1){ 
				for (int i = 0; i <= limTables; i++) {
					sql="Select count(o) as dTypes, o from datatypes_" + i + " where s = ? group by o";
					ps = connection.prepareStatement(sql);
					ps.setString(1, uri);
					rs = ps.executeQuery();
					while (rs.next()) {
						String endPoint = rs.getString("o");
						int countDType = rs.getInt("dTypes");
						if (result.containsKey(endPoint))
							countDType += result.get(endPoint);
						else
							result.put(endPoint, countDType);
					}
				}
			}
			else{
				sql="Select count(o) as dTypes, o from datatypes where s = ? group by o";
				ps = connection.prepareStatement(sql);
				ps.setString(1, uri);
				rs = ps.executeQuery();
				while (rs.next()) {
					String endPoint = rs.getString("o");
					int countDType = rs.getInt("dTypes");
					if (result.containsKey(endPoint))
						countDType += result.get(endPoint);
					else
						result.put(endPoint, countDType);
				}
			}
			
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(DBUtil.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
			
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				// if (conn != null) {
				// conn.close();
				// }
				return result;

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(DBUtil.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		return result;
	}

	private static int getLimTables() {
		int ret = 0;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			Connection connection = getSQLConnection();
			ps = connection.prepareStatement("SHOW TABLES FROM linklion2;");
			rs = ps.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString(1);
				if(tableName.startsWith("datatypes"))
					ret++;
			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(DBUtil.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				// if (conn != null) {
				// conn.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(DBUtil.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		return ret;
	}

	/*
	 * Select sum(countDType), count(uri) from uri2 where indexDataset=(Select
	 * indDS from dataset2 where name='http://crm.rkbexplorer.com/sparql') group
	 * by countDType;
	 */
	public static Map<Integer, Integer> findURI(String endPoint) {

		return null;
	}

	/*
	 * Select uri, countDType from uri2 where indexDataset=(Select indDS from
	 * dataset2 where name='http://crm.rkbexplorer.com/sparql');
	 */
	public static Map<String, Integer> findAllURIDataTypes(String endPoint) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			Connection connection = getSQLConnection();
			ps = connection.prepareStatement(
					"Select uri, countDType from uri2 where indexDataset=(Select indDS from dataset2 where name = ?)");
			ps.setString(1, endPoint);
			rs = ps.executeQuery();
			while (rs.next()) {
				String uri = rs.getString("uri");
				int countDType = rs.getInt("countDType");
				result.put(uri, countDType);
			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(DBUtil.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				// if (conn != null) {
				// conn.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(DBUtil.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		return result;
	}

	private static Connection getSQLConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		// return
		// DriverManager.getConnection("jdbc:mysql://db4free.net/dbsameas?" +
		// "user=firmao&password=sameas");
		// return
		// DriverManager.getConnection("jdbc:mysql://127.0.0.1/linklion2?" +
		// "user=root&password=sameas");
		if (connGeneric != null)
			return connGeneric;
		else {
			// connGeneric =
			// DriverManager.getConnection("jdbc:mysql://139.18.8.63/linklion2?"
			// +
			// "user=root&password=sameas");
			connGeneric = DriverManager
					.getConnection("jdbc:mysql://127.0.0.1/linklion2?" + "user=root&password=sameas");
			return connGeneric;
		}
	}

	public static Set<String> sendSQL(String sql) {
		HashSet<String> result = new HashSet<String>();
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			Connection connection = getSQLConnection();
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				// String res = rs.getString(1) + "\t" + rs.getString(2);
				String res = "";
				try {
					int i = 1;
					while (true) {
						res += "\t" + rs.getString(i++);
					}
				} catch (Exception e) {
				}
				result.add(res);
			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(DBUtil.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				// if (conn != null) {
				// conn.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(DBUtil.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		return result;
	}

}
