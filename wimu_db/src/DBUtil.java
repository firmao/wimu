import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Context;

public class DBUtil {
	static Connection connGeneric;
	static Map<String, Integer> mDatasetIndex = new HashMap<String, Integer>();
	public static Connection getConnection() throws ClassNotFoundException, SQLException
	{
//		ctx = new InitialContext();
//
//		DataSource ds = (DataSource) ctx.lookup("java:comp/env/jdbc/mysql");
//
//		if (ds != null) return ds.getConnection();
		
		Class.forName("com.mysql.jdbc.Driver");
		//return DriverManager.getConnection("jdbc:mysql://db4free.net/dbsameas?" +
        //        "user=firmao&password=sameas");
        //return DriverManager.getConnection("jdbc:mysql://127.0.0.1/linklion2?" +
        //                "user=root&password=sameas");        
        if(connGeneric != null)
        	return connGeneric;
        else{
        	//connGeneric = DriverManager.getConnection("jdbc:mysql://139.18.8.63/linklion2?" +
            //       "user=root&password=sameas");
        	connGeneric = DriverManager.getConnection("jdbc:mysql://127.0.0.1/linklion2?" +
                    "user=root&password=sameas");
        	return connGeneric;
        }
	}
	
	public static void setAutoCommit(boolean value) {
		try {
			getConnection().setAutoCommit(value);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * Table Dataset(int index, string Name)
	 * Table URI(int index, string uri, int indDataset)
	 * @DataSetName = Name of the EndPoint
	 */
	public static void insert(String dataSetName, String subjURI, Integer count) throws ClassNotFoundException, SQLException{
//		Connection conn = null;
//		Context ctx;
//		try {
//			conn = getConnection();
//			int indDataSet =  0;
//			if (mDatasetIndex.containsKey(dataSetName)){
//				indDataSet = mDatasetIndex.get(dataSetName);
//			}else{
//				indDataSet = getLastIndex(conn, "dataset2", "indDS") + 1;
//				
//				PreparedStatement prep = conn
//						.prepareStatement("INSERT INTO linklion2.dataset2 (indDS, name) VALUES (?,?);");
//				prep.setInt(1, indDataSet);
//				prep.setString(2, dataSetName.trim());
//	
//				prep.executeUpdate();
//				mDatasetIndex.put(dataSetName, indDataSet);
//			}
//			int indURI = getLastIndex(conn, "uri2", "indURI") + 1;
//			PreparedStatement prep2 = conn
//					.prepareStatement("INSERT INTO linklion2.uri2 (indURI, uri, indexDataset, countDType) VALUES (?, ?, ?, ?);");
//			prep2.setInt(1, indURI);
//			prep2.setString(2, subjURI.trim());
//			prep2.setInt(3, indDataSet);
//			prep2.setInt(4, count);
//
//			prep2.executeUpdate();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		Connection dbConnection = getConnection();
		CallableStatement callableStatement = null;

		String insertStoreProc = "{call ADD_DB_S(?,?,?)}";

		try {
			callableStatement = dbConnection.prepareCall(insertStoreProc);

			callableStatement.setString(1, subjURI);
			callableStatement.setString(2, dataSetName);
			callableStatement.setInt(3, count);

			callableStatement.executeUpdate();

		} catch (SQLException e) {

			System.err.println(e.getMessage());

		} finally {

			if (callableStatement != null) {
				callableStatement.close();
			}

		}
	}

	public static void insert(String subj, String endPoint) throws ClassNotFoundException, SQLException {
		Connection dbConnection = getConnection();
		CallableStatement callableStatement = null;

		String insertStoreProc = "{call ADD_DB(?,?)}";
		//String insertStoreProc = "{call ADD_DB_OLD(?,?)}";

		try {
			callableStatement = dbConnection.prepareCall(insertStoreProc);

			callableStatement.setString(1, subj);
			callableStatement.setString(2, endPoint);

			callableStatement.executeUpdate();

		} catch (SQLException e) {

			System.err.println(e.getMessage());

		} finally {

			if (callableStatement != null) {
				callableStatement.close();
			}

		}

	}
	
	private static int getLastIndex(Connection conn, String table, String sNameInd) {
		int iRet = 0;
		Statement st = null;
		ResultSet rs = null;

		try {
			st = conn.createStatement();
			String sQuery = "Select MAX("+sNameInd+") as ind from linklion2." + table + ";";
			rs = st.executeQuery(sQuery);
			while (rs.next()) {
				iRet = rs.getInt(1);
			}

		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(DBUtil.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				//if (conn != null) {
				//	conn.close();
				//}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(DBUtil.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

		return iRet;
	}
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException{
		insert("DatasetTest", "http://fdsa.rew.fds", 3);
	}
	
}
