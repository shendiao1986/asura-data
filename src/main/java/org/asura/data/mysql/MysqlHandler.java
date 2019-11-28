package org.asura.data.mysql;

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
import org.asura.data.sql.DeleteSQL;
import org.asura.data.sql.ISQL;
import org.asura.data.sql.InsertSQL;
import org.asura.data.sql.LimitSQL;
import org.asura.data.sql.SelectSQL;
import org.asura.data.sql.UpdateSQL;

public class MysqlHandler {
	private ConnectionInformation ci;
	private static HashMap<String, String[]> keyCache = new HashMap();

	public MysqlHandler() {
		this.ci = ConnectionInformation.fromConfigFile();
	}

	public MysqlHandler(ConnectionInformation ci) {
		this.ci = ci;
	}

	public static void close(ConnectionInformation ci) {
		MysqlConnetionPool.closeConnection(ci);
	}

	public int getCount(String sql) {
		try {
			Statement ps = MysqlConnetionPool.getConnection(this.ci).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

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
			Statement ps = MysqlConnetionPool.getConnection(this.ci).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

			ResultSet resultSet = ps.executeQuery("select count(1) from " + table);

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

	public DataRecord getDataRecordByKey(String table, DataRecord dr) {
		SelectSQL sql = new SelectSQL();
		sql.addTable(table);
		for (String key : getPrimaryKeyMetaData(table)) {
			sql.addWhereCondition(key, dr.getFieldValue(key));
		}

		DataIterator it = select(sql, 1);
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

		DataIterator it = select(sql, 1);
		DataRecord result = null;
		if (it.hasNext()) {
			result = (DataRecord) it.next();
		}

		it.close();

		return result;
	}

	private DataIterator<DataRecord> selectSql(final String sql) {
		return new DataIterator<DataRecord>() {
			private ResultSet resultSet;
			private Statement ps;
			private ResultSetMetaData meta;
			
			{
				initial();
			}

			private void initial() {
				try {
					this.ps = MysqlConnetionPool.getConnection(MysqlHandler.this.ci).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

					this.resultSet = this.ps.executeQuery(sql);
					this.meta = this.resultSet.getMetaData();
				} catch (Exception e) {
					close();
					throw new DataException(MysqlHandler.this.ci + " Failed to initial sql '" + sql + "'\n"
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

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

			public boolean hasNext() {
				try {
					boolean has = this.resultSet.next();
					this.resultSet.previous();
					return has;
				} catch (Exception e) {
					throw new DataException(MysqlHandler.this.ci + " Failed to get whether there is next record.\n"
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

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
						dr.AddField(field, String.valueOf(v));
					}
					return dr;
				} catch (Exception e) {
					throw new DataException(MysqlHandler.this.ci + " Failed to get next record.\n"
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

			public void reset() {
				close();
				initial();
			}
		};
	}

	private List<SelectSQL> getResolvedSQL(SelectSQL sql, int fetchSize) {
		List list = new ArrayList();
        if(sql != null && fetchSize > 0)
        {
            if(!StringUtil.isNullOrEmpty(sql.getSql()))
            {
                list.add(sql);
                return list;
            }
            SelectSQL newSql = sql.clone();
            newSql.getFields().clear();
            newSql.addField("count(*)");
            newSql.setLimit(new LimitSQL());
            DataIterator it = selectSql(newSql.getSQLString(ISQL.DBType.mysql));
            int all = 0;
            if(it.hasNext())
                all = getCount((DataRecord)it.next());
            it.close();
            int start = 0;
            int count = 0;
            if(sql.getLimit() != null)
            {
                start = sql.getLimit().getStart();
                count = sql.getLimit().getCount();
            }
            int page = 1;
            if(count == 0)
            {
                int totalStart = start;
                do
                {
                    newSql = sql.clone();
                    newSql.setLimitStart(totalStart);
                    if(totalStart >= all)
                        break;
                    newSql.setLimitCount(fetchSize);
                    list.add(newSql);
                    totalStart = fetchSize * page++ + start;
                } while(true);
            } else
            {
                int max = start + count;
                for(int totalStart = start; totalStart < max; totalStart = fetchSize * page++ + start)
                {
                    newSql = sql.clone();
                    newSql.setLimitStart(totalStart);
                    if(totalStart >= all)
                        break;
                    newSql.setLimitCount(Math.min(fetchSize, max - totalStart));
                    list.add(newSql);
                }

            }
        }
        return list;

	}

	private int getCount(DataRecord dr) {
		try {
			return Integer.valueOf(dr.getFieldValue(dr.getAllFields()[0])).intValue();
		} catch (Exception e) {
		}
		return 0;
	}

	public List<DataRecord> selectList(SelectSQL sql) {
		List list = new ArrayList();
		DataIterator it = select(sql);
		while (it.hasNext()) {
			list.add((DataRecord) it.next());
		}
		it.close();

		return list;
	}

	public List<DataRecord> selectListDirectly(SelectSQL sql) {
		List list = new ArrayList();
		DataIterator it = selectDirectly(sql);
		while (it.hasNext()) {
			list.add((DataRecord) it.next());
		}
		it.close();

		return list;
	}

	public DataIterator<DataRecord> selectDirectly(final SelectSQL sql) {
		final List list = new ArrayList();
		list.add(sql);

		if (list.size() == 0) {
			return new EmptyDataIterator();
		}

		return new DataIterator<DataRecord>() {
			private int pos;
			private DataIterator<DataRecord> currentIt;
			
			{
				pos=0;
				currentIt = selectSql(((SelectSQL)list.get(0)).getSQLString());

			}

			public void close() {
				this.currentIt.close();
			}

			public boolean hasNext() {
				if (this.currentIt.hasNext()) {
					return this.currentIt.hasNext();
				}
				if (this.pos < list.size() - 1) {
					this.pos += 1;
					this.currentIt.close();
					this.currentIt = MysqlHandler.this
							.selectSql(((SelectSQL) list.get(this.pos)).getSQLString());

					return hasNext();
				}
				this.currentIt.close();
				return false;
			}

			public DataRecord next() {
				return ((DataRecord) this.currentIt.next());
			}

			public void reset() {
				this.pos = 0;
				this.currentIt.close();
				this.currentIt = MysqlHandler.this.selectSql(((SelectSQL) list.get(this.pos)).getSQLString());
			}
		};
	}

	public DataIterator<DataRecord> select(SelectSQL sql, int fetchSize) {
		if (sql == null) {
			return new EmptyDataIterator();
		}
		final List list = getResolvedSQL(sql, fetchSize);

		if (list.size() == 0) {
			return new EmptyDataIterator();
		}

		return new DataIterator<DataRecord>() {
			private int pos;
			private DataIterator<DataRecord> currentIt;
			
			{
				pos = 0;
				currentIt = selectSql(((SelectSQL) list.get(0)).getSQLString());

			}

			public void close() {
				this.currentIt.close();
			}

			public boolean hasNext() {
				if (this.currentIt.hasNext()) {
					return this.currentIt.hasNext();
				}
				if (this.pos < list.size() - 1) {
					this.pos += 1;
					this.currentIt.close();
					this.currentIt = MysqlHandler.this
							.selectSql(((SelectSQL) list.get(this.pos)).getSQLString());

					return hasNext();
				}
				this.currentIt.close();
				return false;
			}

			public DataRecord next() {
				return ((DataRecord) this.currentIt.next());
			}

			public void reset() {
				this.pos = 0;
				this.currentIt.close();
				this.currentIt = MysqlHandler.this.selectSql(((SelectSQL) list.get(this.pos)).getSQLString());
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
					this.con = MysqlConnetionPool.getNewConnection(MysqlHandler.this.ci);
				else
					this.con = MysqlConnetionPool.getConnection(MysqlHandler.this.ci);
			}

			public void addRecord(DataRecord dr) {
				addRecord(dr, false);
			}

			public void addRecords(List<DataRecord> records, boolean override) {
				if (records.size() <= 0)
					return;
				Connection con = MysqlConnetionPool.getNewConnection(MysqlHandler.this.ci);
				try {
					con.setAutoCommit(false);

					for (DataRecord dr : records) {
						addRecord(dr, override, con);
					}

					con.commit();
					con.close();
				} catch (Exception e) {
					throw new DataException(MysqlHandler.this.ci + " Failed to add record '" + records.size() + "'"
							+ ExceptionUtil.getExceptionContent(e));
				} finally {
					try {
						con.close();
					} catch (SQLException localSQLException) {
					}
				}
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
					String sql = insert.getSQLString(ISQL.DBType.mysql);

					PreparedStatement ps = null;
					try {
						ps = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

						ps.execute(sql);
						ps.close();
					} catch (Exception e) {
						con = MysqlConnetionPool.getConnection(MysqlHandler.this.ci);
						try {
							ps = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
							ps.execute(sql);
							ps.close();
						} catch (Exception e1) {
							throw new DataException(MysqlHandler.this.ci + " Failed to add record '" + sql + "'"
									+ ExceptionUtil.getExceptionContent(e));
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

			public void addRecord(DataRecord dr, boolean override) {
				addRecord(dr, override, this.con);
			}

			public void begineTransaction() {
				try {
					this.con.setAutoCommit(false);
				} catch (SQLException e) {
					throw new DataException(MysqlHandler.this.ci + " Failed to begin transactoin "
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

			public void commit() {
				try {
					this.con.commit();
					this.con.setAutoCommit(true);
					this.con.close();
				} catch (SQLException e) {
					throw new DataException(MysqlHandler.this.ci + " Failed to commite transactoin "
							+ ExceptionUtil.getExceptionContent(e));
				}
			}

			public boolean containsRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					SelectSQL sql = new SelectSQL();
					sql.addTable(table);

					for (String key : MysqlHandler.this.getPrimaryKeyMetaData(table)) {
						sql.addWhereCondition(key, dr.getFieldValue(key));
					}

					sql.setLimitCount(1);

					DataIterator it = MysqlHandler.this.select(sql, 1);
					boolean contains = it.hasNext();
					it.close();

					return contains;
				}
				return false;
			}

			public void deleteRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					DeleteSQL delete = new DeleteSQL();
					delete.addTable(table);
					for (String key : MysqlHandler.this.getPrimaryKeyMetaData(table)) {
						delete.addWhereCondition(key, dr.getFieldValue(key));
					}

					PreparedStatement ps = null;
					try {
						ps = this.con.prepareStatement(delete.getSQLString(ISQL.DBType.mysql), ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
						ps.execute(delete.getSQLString(ISQL.DBType.mysql));
						ps.close();
					} catch (SQLException e) {
						throw new DataException(MysqlHandler.this.ci + " Failed to delete record '"
								+ delete.getSQLString(ISQL.DBType.mysql) + "'" + ExceptionUtil.getExceptionContent(e));
					} finally {
						if (ps != null)
							try {
								ps.close();
							} catch (SQLException localSQLException3) {
							}
					}
				}
			}

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

			public void updateRecord(DataRecord dr) {
				if ((dr != null) && (dr.getAllFields().length > 0)) {
					UpdateSQL update = new UpdateSQL();
					update.addTable(table);
					update.addFieldValues(dr);
					for (String key : MysqlHandler.this.getPrimaryKeyMetaData(table)) {
						update.addWhereCondition(key, dr.getFieldValue(key));
					}
					String sql = update.getSQLString(ISQL.DBType.mysql);

					PreparedStatement ps = null;
					try {
						ps = this.con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
						ps.executeUpdate();
						ps.close();
					} catch (SQLException e) {
						throw new DataException(MysqlHandler.this.ci + " Failed to update record '" + sql + "'"
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

			public void addRecords(List<DataRecord> drs) {
				addRecords(drs, false);
			}

			public void deleteRecords(List<DataRecord> drs) {
				if (drs.size() > 0) {
					PreparedStatement ps = null;
					Connection con = MysqlConnetionPool.getNewConnection(MysqlHandler.this.ci);
					try {
						con.setAutoCommit(false);
						ps = con.prepareStatement("select 1", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
						for (DataRecord dr : drs) {
							DeleteSQL delete = new DeleteSQL();
							delete.addTable(table);
							for (String key : MysqlHandler.this.getPrimaryKeyMetaData(table)) {
								delete.addWhereCondition(key, dr.getFieldValue(key));
							}
							String sql = delete.getSQLString(ISQL.DBType.mysql);
							ps.addBatch(sql);
						}

						ps.executeBatch();
						ps.close();
						con.commit();
						con.close();
					} catch (Exception e) {
						throw new DataException(MysqlHandler.this.ci + " Failed to delete record '" + drs.size() + "'"
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

			public void execute(String sql) {
				PreparedStatement ps = null;
				try {
					ps = this.con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
					ps.execute();
					ps.close();
				} catch (SQLException e) {
					throw new DataException(MysqlHandler.this.ci + " Failed to execute sql '" + sql + "'"
							+ ExceptionUtil.getExceptionContent(e));
				} finally {
					if (ps != null)
						try {
							ps.close();
						} catch (SQLException localSQLException1) {
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
			DatabaseMetaData meta = MysqlConnetionPool.getConnection(this.ci).getMetaData();

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
		ArrayList list = new ArrayList();
		try {
			DatabaseMetaData meta = MysqlConnetionPool.getConnection(this.ci).getMetaData();

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
			ps = MysqlConnetionPool.getConnection(this.ci).prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
