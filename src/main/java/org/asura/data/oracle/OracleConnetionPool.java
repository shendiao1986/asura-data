package org.asura.data.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.asura.core.util.StringUtil;
import org.asura.data.mysql.ConnectionInformation;

public class OracleConnetionPool {
	private ConnectionInformation info;
	private Connection con;
	private static HashMap<ConnectionInformation, OracleConnetionPool> map = new HashMap<>();

	private OracleConnetionPool(ConnectionInformation info) {
		this.info = info;
		this.con = getNewConnection(info);
	}

	public static Connection getNewConnection(ConnectionInformation pro) {
		String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=" + pro.getHost() + ")(PORT="
				+ pro.getPort() + "))(CONNECT_DATA=(SERVICE_NAME=" + pro.getDbName() + ")))";
		if (!(StringUtil.isNullOrEmpty(pro.getUrl())))
			url = pro.getUrl();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			return DriverManager.getConnection(url, pro.getUserName(), pro.getPassword());
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static synchronized OracleConnetionPool getInstance(ConnectionInformation info) {
		if (!(map.containsKey(info))) {
			map.put(info, new OracleConnetionPool(info));
		}

		return ((OracleConnetionPool) map.get(info));
	}

	public static Connection getConnection(ConnectionInformation info) {
		return getInstance(info).getConnection();
	}

	public Connection getNewConnection(Connection con) {
		if (checkConnection(con)) {
			return con;
		}
		closeConnection(con);
		return getNewConnection(this.info);
	}

	public static void closeConnection(ConnectionInformation info) {
		if (map.containsKey(info)) {
			closeConnection(((OracleConnetionPool) map.get(info)).con);
			map.remove(info);
		}
	}

	private static void closeConnection(Connection con) {
		if (con == null)
			return;
		try {
			con.close();
		} catch (SQLException localSQLException) {
		}
	}

	private Connection getConnection() {
		if (!(checkConnection(this.con))) {
			closeConnection(this.con);
			this.con = getNewConnection(this.info);
		}

		return this.con;
	}

	private boolean checkConnection(Connection con) {
		Statement pingStatement = null;

		if (con == null)
			return false;
		try {
			if (con.isClosed()) {
				return false;
			}

		} catch (SQLException e) {
			return false;
		} finally {
			if (pingStatement != null)
				try {
					pingStatement.close();
				} catch (SQLException localSQLException3) {
				}
		}
		return true;
	}
}
