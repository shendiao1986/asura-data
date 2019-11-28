package org.asura.data.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class SqliteConnection {
	private String path;
	private Connection con;
	private static HashMap<String, SqliteConnection> map = new HashMap();

	private SqliteConnection(String path) {
		this.path = path;
		this.con = getNewInstance(path);
	}

	public static void removeConnnection(String path) {
		SqliteConnection connection = (SqliteConnection) map.get(path);
		map.remove(path);
		if (connection == null)
			return;
		try {
			connection.con.close();
		} catch (SQLException localSQLException) {
		}
	}

	private Connection getNewInstance(String path) {
		try {
			Class.forName("org.sqlite.JDBC");
			return DriverManager.getConnection("jdbc:sqlite:" + path);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static SqliteConnection getInstance(String path) {
		if (!(map.containsKey(path))) {
			map.put(path, new SqliteConnection(path));
		}

		return ((SqliteConnection) map.get(path));
	}

	public static Connection getConnection(String path) {
		return getInstance(path).getConnection();
	}

	public Connection getNewConnection(Connection con) {
		if (checkConnection(con)) {
			return con;
		}
		closeConnection(con);
		return getNewInstance(this.path);
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
			this.con = getNewInstance(this.path);
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
		if (pingStatement != null) {
			try {
				pingStatement.close();
			} catch (SQLException localSQLException4) {
			}
		}
		return true;
	}
}
