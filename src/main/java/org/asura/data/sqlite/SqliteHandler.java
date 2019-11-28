package org.asura.data.sqlite;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.asura.core.data.DataIterator;
import org.asura.core.data.DataRecord;
import org.asura.core.data.IEditor;
import org.asura.core.exception.DataException;
import org.asura.core.util.ExceptionUtil;
import org.asura.core.util.StringUtil;
import org.asura.data.sql.DeleteSQL;
import org.asura.data.sql.ISQL;
import org.asura.data.sql.SQLUtils;
import org.asura.data.sql.SelectSQL;
import org.asura.data.sql.UpdateSQL;

public class SqliteHandler {
	private String path;
	private static HashMap<String, String[]> keyCache = new HashMap();

	public SqliteHandler(String path) {
		this.path = path;
	}

	public DataRecord getDataRecordByKey(String table, DataRecord dr) {
		SelectSQL sql = new SelectSQL();
		sql.addTable(table);
		for (String key : getPrimaryKeyMetaData(table)) {
			sql.addWhereCondition(key, dr.getFieldValue(key));
		}

		DataIterator it = select(sql.getSQLString());
		DataRecord result = null;
		if (it.hasNext()) {
			result = (DataRecord) it.next();
		}

		it.close();

		return result;
	}

	public DataIterator<DataRecord> select(SelectSQL sql) {
		return select(sql.getSQLString());
	}

	public DataIterator<DataRecord> select(final String sql) {
		return new DataIterator<DataRecord>() {
			private ResultSet resultSet;
			private Statement ps;
			private ResultSetMetaData meta;

			{
				initial();
			}

			private void initial() {
				try {
					this.ps = SqliteConnection.getConnection(SqliteHandler.this.path).createStatement(1003, 1007);

					this.resultSet = this.ps.executeQuery(sql);
					this.meta = this.resultSet.getMetaData();
				} catch (Exception e) {
					close();
					throw new DataException(
							"Failed to initial sql '" + sql + "'\n" + ExceptionUtil.getExceptionContent(e));
				}
			}

			@Override
			public boolean hasNext() {
				try {
					boolean has = this.resultSet.next();
					if (!(has)) {
						close();
					}
					return has;
				} catch (Exception e) {
					throw new DataException(
							"Failed to get whether there is next record.\n" + ExceptionUtil.getExceptionContent(e));
				}
			}

			@Override
			public DataRecord next() {
				try {
					DataRecord dr = new DataRecord();
					for (int i = 1; i <= this.meta.getColumnCount(); ++i) {
						dr.AddField(this.meta.getColumnName(i),
								String.valueOf(this.resultSet.getObject(this.meta.getColumnName(i))));
					}
					return dr;
				} catch (Exception e) {
					throw new DataException("Failed to get next record.\n" + ExceptionUtil.getExceptionContent(e));
				}
			}

			@Override
			public void close() {
				if (this.ps != null)
					try {
						this.ps.close();
					} catch (SQLException localSQLException) {
					}
				if (this.resultSet == null)
					return;
				try {
					this.resultSet.close();
				} catch (SQLException localSQLException1) {
				}
			}

			@Override
			public void reset() {
				close();
				initial();
			}
		};
	}

	public IEditor editor(final String table) {
		return new IEditor() {
			private Connection con;
			
			
			private void initial() {
				this.con = SqliteConnection.getConnection(SqliteHandler.this.path);
			}
			
			@Override
			public void updateRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					UpdateSQL update = new UpdateSQL();
					update.addTable(table);
					update.addFieldValues(dr);
					for (String key : SqliteHandler.this.getPrimaryKeyMetaData(table)) {
						update.addWhereCondition(key, dr.getFieldValue(key));
					}
					String sql = update.getSQLString(ISQL.DBType.mysql);

					PreparedStatement ps = null;
					try {
						ps = this.con.prepareStatement(sql, 1008);
						ps.executeUpdate();
						ps.close();
					} catch (SQLException e) {
						throw new DataException(
								"Failed to update record '" + sql + "'" + ExceptionUtil.getExceptionContent(e));
					} finally {
						if (ps != null)
							try {
								ps.close();
							} catch (SQLException localSQLException1) {
							}
					}
				}
			}
			
			@Override
			public void processRecord(DataRecord dr) {
				switch (dr.getDataAction()) {
				case Add:
					addRecord(dr);
					break;
				case Delete:
					deleteRecord(dr);
					break;
				case Update:
					updateRecord(dr);
				}
			}

			
			@Override
			public void execute(String paramString) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void deleteRecords(List<DataRecord> paramList) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void deleteRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					DeleteSQL delete = new DeleteSQL();
					delete.addTable(table);
					for (String key : SqliteHandler.this.getPrimaryKeyMetaData(table)) {
						delete.addWhereCondition(key, dr.getFieldValue(key));
					}

					Statement ps = null;
					try {
						ps = this.con.createStatement();
						ps.execute(delete.getSQLString(ISQL.DBType.mysql));
						ps.close();
					} catch (SQLException e) {
						throw new DataException("Failed to delete record '" + delete.getSQLString(ISQL.DBType.mysql)
								+ "'" + ExceptionUtil.getExceptionContent(e));
					} finally {
						if (ps != null)
							try {
								ps.close();
							} catch (SQLException localSQLException3) {
							}
					}
				}
			}
			
			@Override
			public boolean containsRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					SelectSQL sql = new SelectSQL();
					sql.addTable(table);

					for (String key : SqliteHandler.this.getPrimaryKeyMetaData(table)) {
						sql.addWhereCondition(key, dr.getFieldValue(key));
					}

					sql.setLimitCount(1);

					DataIterator it = SqliteHandler.this.select(sql);
					boolean contains = it.hasNext();
					it.close();

					return contains;
				}
				return false;
			}
			
			@Override
			public void commit() {
				try {
					this.con.commit();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			@Override
			public void begineTransaction() {
				try {
					initial();
					this.con.setAutoCommit(false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			@Override
			public void addRecords(List<DataRecord> paramList, boolean paramBoolean) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void addRecords(List<DataRecord> paramList) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void addRecord(DataRecord dr, boolean override) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					String sql = "insert into " + table + " ";
					List keys = new ArrayList();
					List values = new ArrayList();
					for (String field : dr.getAllFields()) {
						keys.add(field);
						values.add("'" + SQLUtils.getStandardSQLValue(dr.getFieldValue(field)) + "'");
					}

					sql = sql + "(" + StringUtil.getStringFromStrings(keys, ",") + ")";
					sql = sql + " values " + "(" + StringUtil.getStringFromStrings(values, ",") + ")";

					Statement ps = null;
					try {
						ps = this.con.createStatement();

						ps.execute(sql);
						ps.close();
					} catch (Exception e) {
						this.con = SqliteConnection.getConnection(SqliteHandler.this.path);
						try {
							ps = this.con.createStatement();
							ps.execute(sql);
							ps.close();
						} catch (Exception e1) {
							throw new DataException(
									"Failed to add record '" + sql + "'" + ExceptionUtil.getExceptionContent(e));
						}
					} finally {
						if (ps != null)
							try {
								ps.close();
							} catch (SQLException localSQLException1) {
							}
					}
				}
			}

			
			@Override
			public void addRecord(DataRecord dr) {
				try {
					addRecord(dr, false);
				} catch (DataException e) {
					if (!(e.getMessage().contains("is not unique")))
						throw e;
				}
			}
		};
	}

	public DataRecord getRecordByKey(String table, DataRecord dr) {
		SelectSQL sql = new SelectSQL();
		sql.addTable(table);
		for (String field : getPrimaryKeyMetaData(table)) {
			String value = dr.getFieldValue(field);
			if (!(StringUtil.isNullOrEmpty(value))) {
				sql.addWhereCondition(field, value);
			}
		}
		sql.setLimitCount(1);
		DataIterator it = select(sql);
		DataRecord result = null;
		if (it.hasNext()) {
			result = (DataRecord) it.next();
		}
		it.close();

		return result;
	}

	public String[] getTables() {
		ArrayList list = new ArrayList();
		try {
			DatabaseMetaData meta = SqliteConnection.getConnection(this.path).getMetaData();

			ResultSet rs = meta.getTables(null, null, null, new String[] { "TABLE" });
			while (rs.next()) {
				list.add(rs.getString(3));
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ((String[]) list.toArray(new String[0]));
	}

	private String[] getPrimaryKeyMetaData(String tableName) {
		if (keyCache.containsKey(this.path + tableName)) {
			return ((String[]) keyCache.get(this.path + tableName));
		}
		ArrayList list = new ArrayList();
		try {
			DatabaseMetaData meta = SqliteConnection.getConnection(this.path).getMetaData();

			ResultSet rs = meta.getPrimaryKeys(null, null, tableName);
			while (rs.next()) {
				list.add(rs.getString(4));
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		keyCache.put(this.path + tableName, (String[]) list.toArray(new String[0]));
		return ((String[]) list.toArray(new String[0]));
	}

	public void execute(String sql) {
		PreparedStatement ps = null;
		try {
			ps = SqliteConnection.getConnection(this.path).prepareStatement(sql, 1008);
			ps.execute();
			ps.close();
		} catch (SQLException e) {
			throw new DataException("Failed to execute sql '" + sql + "'" + ExceptionUtil.getExceptionContent(e));
		} finally {
			if (ps != null)
				try {
					ps.close();
				} catch (SQLException localSQLException1) {
				}
		}
	}
}
