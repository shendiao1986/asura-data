package org.asura.data.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class MysqlConnetionPool {
	private ConnectionInformation info;
	private Connection con;
	private static HashMap<ConnectionInformation, MysqlConnetionPool> map = new HashMap();

	private MysqlConnetionPool(ConnectionInformation info) {
		this.info = info;
		this.con = getNewConnection(info);
	}

	public static Connection getNewConnection(ConnectionInformation pro) {
		String url = "jdbc:mysql://" + pro.getHost() + ":" + pro.getPort() + "/" + pro.getDbName()
				+ "?autoReconnect=true&useUnicode=true&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection(url, pro.getUserName(), pro.getPassword());
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static synchronized MysqlConnetionPool getInstance(ConnectionInformation info) {
		if (!(map.containsKey(info))) {
			map.put(info, new MysqlConnetionPool(info));
		}

		return ((MysqlConnetionPool) map.get(info));
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
			closeConnection(((MysqlConnetionPool) map.get(info)).con);
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

		if (con == null) {
			return false;
		}

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

		try {
			pingStatement = con.createStatement();
			pingStatement.executeQuery("SELECT 1").close();
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
