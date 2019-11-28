package org.asura.data.oracle;

import java.io.BufferedReader;
import java.io.Reader;
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
import org.asura.core.data.EmptyDataIterator;
import org.asura.core.data.IEditor;
import org.asura.core.exception.DataException;
import org.asura.core.util.ExceptionUtil;
import org.asura.core.util.StringUtil;
import org.asura.data.mysql.ConnectionInformation;
import org.asura.data.sql.DeleteSQL;
import org.asura.data.sql.ISQL;
import org.asura.data.sql.ISQL.DBType;
import org.asura.data.sql.InsertSQL;
import org.asura.data.sql.SelectSQL;
import org.asura.data.sql.UpdateSQL;

import oracle.sql.CLOB;

public class OracleHandler {
	private ConnectionInformation ci;
	private static HashMap<String, String[]> keyCache = new HashMap<>();

	public OracleHandler() {
		this.ci = ConnectionInformation.fromConfigFile();
	}

	public OracleHandler(ConnectionInformation ci) {
		this.ci = ci;
	}

	public static void close(ConnectionInformation ci) {
		OracleConnetionPool.closeConnection(ci);
	}

	public int getCount(String sql) {
		try {
			Statement ps = OracleConnetionPool.getConnection(this.ci).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_READ_ONLY);

			ResultSet resultSet = ps.executeQuery(sql);

			resultSet.next();

			Long count = (Long) resultSet.getObject(1);
			try {
				ps.close();
			} catch (Exception localException1) {
			}
			try {
				resultSet.close();
			} catch (Exception localException2) {
			}
			return count.intValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	public int getRecordsCount(String table) {
		try {
			Statement ps = OracleConnetionPool.getConnection(this.ci).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_READ_ONLY);

			ResultSet resultSet = ps.executeQuery("select count(1) from " + table);

			resultSet.next();

			Long count = (Long) resultSet.getLong(1);
			try {
				ps.close();
			} catch (Exception localException1) {
			}
			try {
				resultSet.close();
			} catch (Exception localException2) {
			}
			return count.intValue();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	public DataRecord getDataRecordByKey(String table, DataRecord dr) {
		SelectSQL sql = new SelectSQL();
		sql.addTable(table);
		for (String key : getPrimaryKeyMetaData(table)) {
			sql.addWhereCondition(key, dr.getFieldValue(key));
		}

		DataIterator<DataRecord> it = select(sql, 1);
		DataRecord result = null;
		if (it.hasNext()) {
			result = (DataRecord) it.next();
		}

		it.close();

		return result;
	}

	public DataRecord getDataRecordByCondition(String table, DataRecord dr) {
		SelectSQL sql = new SelectSQL();
		sql.addTable(table);
		for (String key : dr.getAllFields()) {
			sql.addWhereCondition(key, dr.getFieldValue(key));
		}

		DataIterator<DataRecord> it = select(sql, 1);
		DataRecord result = null;
		if (it.hasNext()) {
			result = (DataRecord) it.next();
		}

		it.close();

		return result;
	}

	private DataIterator<DataRecord> selectSql(final String sql, int fetchSize) {

		return new DataIterator<DataRecord>() {
			private ResultSet resultSet;
			private Statement ps;
			private ResultSetMetaData meta;

			{
				initial();
			}

			private void initial() {
				try {
					this.ps = OracleConnetionPool.getConnection(OracleHandler.this.ci).createStatement(1005, 1007);

					this.resultSet = this.ps.executeQuery(sql);
					this.meta = this.resultSet.getMetaData();
				} catch (Exception e) {
					close();
					throw new DataException(OracleHandler.this.ci + " Failed to initial sql '" + sql + "'\n"
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

			@Override
			public boolean hasNext() {
				try {
					boolean has = this.resultSet.next();
					this.resultSet.previous();
					return has;
				} catch (Exception e) {
					throw new DataException(OracleHandler.this.ci + " Failed to get whether there is next record.\n"
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

			@Override
			public DataRecord next() {
				try {
					this.resultSet.next();
					DataRecord dr = new DataRecord();
					for (int i = 1; i <= this.meta.getColumnCount(); ++i) {
						Object v = this.resultSet.getObject(this.meta.getColumnName(i));
						String field = this.meta.getColumnName(i);
						if (v == null) {
							v = "";
						}
						if (!(field.equals("RNNNNNN"))) {
							if (v instanceof CLOB)
								dr.AddField(field, clobToString((CLOB) v));
							else {
								dr.AddField(field, String.valueOf(v));
							}
						}
					}
					return dr;
				} catch (Exception e) {
					throw new DataException(OracleHandler.this.ci + " Failed to get next record.\n"
							+ ExceptionUtil.getExceptionContent(e));
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

	private String clobToString(CLOB clob) throws Exception {
		String reString = "";
		Reader is = clob.getCharacterStream();
		BufferedReader br = new BufferedReader(is);
		String s = br.readLine();
		StringBuffer sb = new StringBuffer();
		while (s != null) {
			sb.append(s);
			s = br.readLine();
		}
		reString = sb.toString();
		return reString;
	}

	private List<SelectSQL> getResolvedSQL(SelectSQL sql, int fetchSize) {
		List<SelectSQL> list = new ArrayList<>();
		list.add(sql);

		return list;
	}

	public List<DataRecord> selectList(SelectSQL sql) {
		List<DataRecord> list = new ArrayList<>();
		DataIterator<DataRecord> it = select(sql);
		while (it.hasNext()) {
			list.add(it.next());
		}
		it.close();

		return list;
	}

	public List<DataRecord> selectListDirectly(SelectSQL sql) {
		List<DataRecord> list = new ArrayList<>();
		DataIterator<DataRecord> it = selectDirectly(sql);
		while (it.hasNext()) {
			list.add((DataRecord) it.next());
		}
		it.close();

		return list;
	}

	public DataIterator<DataRecord> selectDirectly(SelectSQL sql) {
		final List<SelectSQL> list = new ArrayList<>();
		list.add(sql);

		if (list.size() == 0) {
			return new EmptyDataIterator<DataRecord>();
		}

		return new DataIterator<DataRecord>() {
			private int pos;
			private DataIterator<DataRecord> currentIt;

			{
				pos = 0;
				currentIt = selectSql(((SelectSQL) list.get(0)).getSQLString(DBType.oracle), 303240);
			}

			@Override
			public boolean hasNext() {
				if (this.currentIt.hasNext()) {
					return this.currentIt.hasNext();
				}
				if (this.pos < list.size() - 1) {
					this.pos += 1;
					this.currentIt.close();
					this.currentIt = OracleHandler.this
							.selectSql(((SelectSQL) list.get(this.pos)).getSQLString(ISQL.DBType.oracle), 100000);

					return hasNext();
				}
				this.currentIt.close();
				return false;
			}

			@Override
			public DataRecord next() {
				return ((DataRecord) this.currentIt.next());
			}

			@Override
			public void close() {
				this.currentIt.close();
			}

			@Override
			public void reset() {
				this.pos = 0;
				this.currentIt.close();
				this.currentIt = OracleHandler.this.selectSql(((SelectSQL) list.get(this.pos)).getSQLString(), 100000);
			}
		};
	}

	public DataIterator<DataRecord> select(SelectSQL sql, final int fetchSize) {
		if (sql == null) {
			return new EmptyDataIterator<DataRecord>();
		}
		final List<SelectSQL> list = getResolvedSQL(sql, fetchSize);

		if (list.size() == 0) {
			return new EmptyDataIterator<DataRecord>();
		}

		return new DataIterator<DataRecord>() {

			private int pos;
			private DataIterator<DataRecord> currentIt;

			{
				pos = 0;
				currentIt = selectSql(((SelectSQL) list.get(0)).getSQLString(DBType.oracle), fetchSize);
			}

			@Override
			public boolean hasNext() {
				if (this.currentIt.hasNext()) {
					return this.currentIt.hasNext();
				}
				if (this.pos < list.size() - 1) {
					this.pos += 1;
					this.currentIt.close();
					this.currentIt = OracleHandler.this
							.selectSql(((SelectSQL) list.get(this.pos)).getSQLString(ISQL.DBType.oracle), fetchSize);

					return hasNext();
				}
				this.currentIt.close();
				return false;
			}

			@Override
			public DataRecord next() {
				return ((DataRecord) this.currentIt.next());
			}

			@Override
			public void close() {
				this.currentIt.close();
			}

			@Override
			public void reset() {
				this.pos = 0;
				this.currentIt.close();
				this.currentIt = OracleHandler.this
						.selectSql(((SelectSQL) list.get(this.pos)).getSQLString(ISQL.DBType.oracle), fetchSize);
			}

		};

	}

	public IEditor editor(String table) {
		return editor(table, false);
	}

	public IEditor editorOnlyTransaction(String table) {
		return editor(table, true);
	}

	private IEditor editor(final String table, final boolean newCon) {

		return new IEditor() {
			private Connection con;

			{
				initial();
			}

			private void initial() {
				if (newCon)
					this.con = OracleConnetionPool.getNewConnection(OracleHandler.this.ci);
				else
					this.con = OracleConnetionPool.getConnection(OracleHandler.this.ci);
			}

			@Override
			public void updateRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					UpdateSQL update = new UpdateSQL();
					update.addTable(table);
					update.addFieldValues(dr);
					for (String key : OracleHandler.this.getPrimaryKeyMetaData(table)) {
						update.addWhereCondition(key, dr.getFieldValue(key));
					}
					String sql = update.getSQLString(ISQL.DBType.oracle);

					PreparedStatement ps = null;
					try {
						ps = this.con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
								ResultSet.CONCUR_UPDATABLE);
						ps.executeUpdate();
						ps.close();
					} catch (SQLException e) {
						throw new DataException(OracleHandler.this.ci + " Failed to update record '" + sql + "'"
								+ ExceptionUtil.getExceptionContent(e));
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
			public void execute(String sql) {
				PreparedStatement ps = null;
				try {
					ps = this.con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
					ps.execute();
					ps.close();
				} catch (SQLException e) {
					throw new DataException(OracleHandler.this.ci + " Failed to execute sql '" + sql + "'"
							+ ExceptionUtil.getExceptionContent(e));
				} finally {
					if (ps != null)
						try {
							ps.close();
						} catch (SQLException localSQLException1) {
						}
				}
			}

			@Override
			public void deleteRecords(List<DataRecord> drs) {
				if (drs.size() > 0) {
					PreparedStatement ps = null;
					Connection con = OracleConnetionPool.getNewConnection(OracleHandler.this.ci);
					try {
						con.setAutoCommit(false);
						ps = con.prepareStatement("select 1", ResultSet.TYPE_SCROLL_SENSITIVE,
								ResultSet.CONCUR_UPDATABLE);
						for (DataRecord dr : drs) {
							DeleteSQL delete = new DeleteSQL();
							delete.addTable(table);
							for (String key : OracleHandler.this.getPrimaryKeyMetaData(table)) {
								delete.addWhereCondition(key, dr.getFieldValue(key));
							}
							String sql = delete.getSQLString(ISQL.DBType.oracle);
							ps.addBatch(sql);
						}

						ps.executeBatch();
						ps.close();
						con.commit();
						con.close();
					} catch (Exception e) {
						throw new DataException(OracleHandler.this.ci + " Failed to delete record '" + drs.size() + "'"
								+ ExceptionUtil.getExceptionContent(e));
					} finally {
						if (ps != null)
							try {
								ps.close();
							} catch (SQLException localSQLException) {
							}
					}
				}
			}

			@Override
			public void deleteRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					DeleteSQL delete = new DeleteSQL();
					delete.addTable(table);
					for (String key : OracleHandler.this.getPrimaryKeyMetaData(table)) {
						delete.addWhereCondition(key, dr.getFieldValue(key));
					}

					PreparedStatement ps = null;
					try {
						ps = this.con.prepareStatement(delete.getSQLString(ISQL.DBType.oracle),
								ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
						ps.execute(delete.getSQLString(ISQL.DBType.oracle));
						ps.close();
					} catch (SQLException e) {
						throw new DataException(OracleHandler.this.ci + " Failed to delete record '"
								+ delete.getSQLString(ISQL.DBType.oracle) + "'" + ExceptionUtil.getExceptionContent(e));
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

					for (String key : OracleHandler.this.getPrimaryKeyMetaData(table)) {
						sql.addWhereCondition(key, dr.getFieldValue(key));
					}

					sql.setLimitCount(1);

					DataIterator<DataRecord> it = OracleHandler.this.select(sql, 1);
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
					this.con.setAutoCommit(true);
					this.con.close();
				} catch (SQLException e) {
					throw new DataException(OracleHandler.this.ci + " Failed to commite transactoin "
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

			@Override
			public void begineTransaction() {
				try {
					this.con.setAutoCommit(false);
				} catch (SQLException e) {
					throw new DataException(OracleHandler.this.ci + " Failed to begin transactoin "
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

			@Override
			public void addRecords(List<DataRecord> records, boolean override) {
				if (records.size() <= 0)
					return;
				Connection con = OracleConnetionPool.getNewConnection(OracleHandler.this.ci);
				try {
					con.setAutoCommit(false);

					for (DataRecord dr : records) {
						addRecord(dr, override, con);
					}

					con.commit();
					con.close();
				} catch (Exception e) {
					throw new DataException(OracleHandler.this.ci + " Failed to add record '" + records.size() + "'"
							+ ExceptionUtil.getExceptionContent(e));
				} finally {
					try {
						con.close();
					} catch (SQLException localSQLException) {
					}
				}
			}

			@Override
			public void addRecords(List<DataRecord> drs) {
				addRecords(drs, false);
			}

			@Override
			public void addRecord(DataRecord dr, boolean override) {
				addRecord(dr, override, this.con);
			}

			@Override
			public void addRecord(DataRecord dr) {
				addRecord(dr, false);
			}

			private void addRecord(DataRecord dr, boolean override, Connection con) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					InsertSQL insert = new InsertSQL();
					insert.addTable(table);
					if (override) {
						insert.setIgnore(false);
						insert.setOverride(true);
					} else {
						insert.setIgnore(true);
						insert.setOverride(false);
					}
					insert.addFieldValue(dr);
					String sql = insert.getSQLString(ISQL.DBType.oracle);

					Statement ps = null;
					try {
						ps = con.createStatement(1005, 1008);

						ps.execute(sql);
						ps.close();
					} catch (Exception e) {
						con = OracleConnetionPool.getConnection(OracleHandler.this.ci);
						try {
							ps = con.prepareStatement(sql, 1008);
							ps.execute(sql);
							ps.close();
						} catch (Exception e1) {
							throw new DataException(OracleHandler.this.ci + " Failed to add record '" + sql + "'"
									+ ExceptionUtil.getExceptionContent(e) + "\n"
									+ ExceptionUtil.getExceptionContent(e1));
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
		};
	}

	public DataIterator<DataRecord> select(SelectSQL sql) {
		return select(sql, 100000);
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
		DataIterator<DataRecord> it = select(sql);
		DataRecord result = null;
		if (it.hasNext()) {
			result = (DataRecord) it.next();
		}
		it.close();

		return result;
	}

	public String[] getTables() {
		ArrayList<String> list = new ArrayList<>();
		try {
			DatabaseMetaData meta = OracleConnetionPool.getConnection(this.ci).getMetaData();

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

	public String[] getPrimaryKeyMetaData(String tableName) {
		if (keyCache.containsKey(this.ci.getDbName() + tableName)) {
			return ((String[]) keyCache.get(this.ci.getDbName() + tableName));
		}
		ArrayList<String> list = new ArrayList<>();
		try {
			DatabaseMetaData meta = OracleConnetionPool.getConnection(this.ci).getMetaData();

			ResultSet rs = meta.getPrimaryKeys(null, null, tableName);
			while (rs.next()) {
				list.add(rs.getString(4));
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		keyCache.put(this.ci.getDbName() + tableName, (String[]) list.toArray(new String[0]));
		return ((String[]) list.toArray(new String[0]));
	}

	public void execute(String sql) {
		PreparedStatement ps = null;
		try {
			ps = OracleConnetionPool.getConnection(this.ci).prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			ps.execute();
			ps.close();
		} catch (SQLException e) {
			throw new DataException(
					this.ci + " Failed to execute sql '" + sql + "'" + ExceptionUtil.getExceptionContent(e));
		} finally {
			if (ps != null)
				try {
					ps.close();
				} catch (SQLException localSQLException1) {
				}
		}
	}
}
