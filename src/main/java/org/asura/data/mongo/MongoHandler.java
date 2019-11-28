package org.asura.data.mongo;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.asura.core.data.DataIterator;
import org.asura.core.data.DataRecord;
import org.asura.core.data.IEditor;
import org.asura.core.util.StringUtil;
import org.asura.core.util.cache.SimpleCache;
import org.asura.data.sql.DeleteSQL;
import org.asura.data.sql.ISQL;
import org.asura.data.sql.LimitSQL;
import org.asura.data.sql.SelectSQL;
import org.asura.data.sql.WhereSQL;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.UpdateOptions;

public class MongoHandler {
	private MongoConnection connection;
	private static SimpleCache<String, MongoCollection<Document>> cache = new SimpleCache<>(100000);
	private static SimpleCache<String, MongoClient> dbCache = new SimpleCache<>(10000);

	public MongoHandler(MongoConnection con) {
		this.connection = con;

	}

	public MongoHandler(String con) {
		this.connection = new MongoConnection(con);
	}

	public MongoHandler() {
		this.connection = MongoConnection.fromConfig();
	}

	public void close() {
		getMongo().close();
		dbCache.remove(this.connection.getServers());
	}

	public long getCount(String dbName, String table) {
		return getTable(dbName, table).count();
	}

	public MongoCollection<Document> getCollection(String dbName, String table) {
		return getTable(dbName, table);
	}

	public long getCount(String dbName, SelectSQL sql) {
		long count = 0l;
		MongoCollection<Document> table = getTable(dbName, sql.getTables().getTable(0));
		Bson ob = getWhere(sql.getWhere());
		if (sql.getLimit() != null) {
			LimitSQL limit = sql.getLimit();
			CountOptions countOptions = new CountOptions();
			if (limit.getStart() > 0) {
				countOptions.skip(limit.getStart());
			}

			if (limit.getCount() > 0) {
				countOptions.limit(limit.getCount());
			}
			count = table.count(ob, countOptions);
		} else {
			count = table.count(ob);
		}
		return count;
	}

	public void deleteTable(String dbName, String table) {
		getTable(dbName, table).drop();
	}

	public void deleteDataBase(String dbName) {
		getMongo().getDatabase(dbName).drop();
	}

	public String[] getTables(String dbName) {
		List<String> tables = new ArrayList<>();
		for (String table : getMongo().getDatabase(dbName).listCollectionNames()) {
			tables.add(table);
		}
		return tables.toArray(new String[0]);
	}

	public String[] getDbs() {
		List<String> dbs = new ArrayList<>();
		for (String db : getMongo().listDatabaseNames()) {
			dbs.add(db);
		}
		return dbs.toArray(new String[0]);
	}

	public void clearEmptyDbs() {
		for (String db : getDbs())
			if (!getMongo().getDatabase(db).listCollectionNames().iterator().hasNext()) {
				getMongo().dropDatabase(db);
			}
	}

	private MongoClient getMongo() {
		if (dbCache.get(this.connection.getServers()) == null) {
			MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(1000).maxWaitTime(5000)
					.socketTimeout(0).connectTimeout(15000).threadsAllowedToBlockForConnectionMultiplier(5000).build();

			MongoClient mongo = new MongoClient(this.connection.parse(), options);

			dbCache.cache(this.connection.getServers(), mongo, 36000);
		}

		return dbCache.get(this.connection.getServers());
	}

	private MongoCollection<Document> getTable(String dbName, String table) {
		String key = dbName + "-" + table;
		if (cache.get(key)==null) {
			cache.cache(key, getMongo().getDatabase(dbName).getCollection(table), 100);
		}

		return cache.get(key);
	}

	public long delete(String dbName, DeleteSQL sql) {
		MongoCollection<Document> table = getTable(dbName, sql.getTables().getTable(0));
		table = table.withWriteConcern(WriteConcern.ACKNOWLEDGED);
		Bson ob = getWhere(sql.getWhere());
		if (ob == null) {
			long count = table.count();
			table.drop();
			return count;
		}
		return table.deleteMany(ob).getDeletedCount();
	}

	public List<DataRecord> selectList(String dbName, SelectSQL sql) {
		List<DataRecord> list = new ArrayList<>();
		DataIterator<DataRecord> it = select(dbName, sql);
		while (it.hasNext()) {
			list.add((DataRecord) it.next());
		}
		it.close();

		return list;
	}

	private Bson getWhere(WhereSQL sql) {
		BasicDBList list = new BasicDBList();
		for (ISQL where : sql.getConditions().getFieldMap()) {
			Bson ob = MongoConverter.convert(where);
			if (ob != null) {
				list.add(ob);
			}
		}

		if (list.size() > 0) {
			return new BasicDBObject("$and", list);
		}
		return null;
	}

	public DataIterator<DataRecord> select(String dbName, SelectSQL sql) {
		return select(dbName, sql, 100000);
	}

	private FindIterable<Document> find(MongoCollection<Document> table, SelectSQL sql) {
		Bson ob = getWhere(sql.getWhere());

		BasicDBObject keys = null;
		if ((sql.getFields() != null) && (sql.getFields().size() > 0)) {
			keys = new BasicDBObject();
			for (String f : sql.getFields().getFields()) {
				keys.put(f, Integer.valueOf(1));
			}

			keys.put("_id", Integer.valueOf(0));
		}

		FindIterable<Document> findIterable = ob != null ? table.find(ob).projection(keys)
				: table.find().projection(keys);

		if (sql.getLimit() != null) {
			LimitSQL limit = sql.getLimit();
			if (limit.getStart() > 0) {
				findIterable = findIterable.skip(limit.getStart());
			}

			if (limit.getCount() > 0) {
				findIterable = findIterable.limit(limit.getCount());
			}
		}

		if (sql.getOrderBy() != null) {
			BasicDBObject order = new BasicDBObject();
			for (String s : sql.getOrderBy().getFields().getFields()) {
				if (!(s.contains("desc")))
					order.put(s, Integer.valueOf(1));
				else {
					order.put(s.replace(" desc ", "").trim(), Integer.valueOf(-1));
				}
			}

			findIterable = findIterable.sort(order);
		}

		return findIterable;
	}

	public DataIterator<DataRecord> select(final String dbName, final SelectSQL sql, final int fetchSize) {
		return new DataIterator<DataRecord>() {
			private MongoCursor<Document> cursor;
			private FindIterable<Document> findIt;
			private Iterator<Document> it;
			{
				if (Arrays.asList(getDbs()).contains(dbName)) {
					MongoCollection<Document> table = getTable(dbName, sql.getTables().getTable(0));
					findIt = find(table, sql);
					findIt.noCursorTimeout(true);
					findIt.batchSize(fetchSize);
					cursor = findIt.iterator();
					it = cursor;
				} else {
					it = new EmptyDbIterator();
				}

			}

			public void close() {
				if (this.it != null) {
					this.it = null;
				}

				if (this.cursor != null) {
					cursor.close();
					cursor = null;
				}
			}

			public boolean hasNext() {
				return this.it.hasNext();
			}

			public DataRecord next() {
				DataRecord dr = new DataRecord();
				Document ob = this.it.next();
				for (String key : ob.keySet()) {
					if (!(key.equals("_id"))) {
						if ((ob.get(key) != null))
							dr.AddField(key, ob.get(key));
						else {
							dr.AddField(key, "");
						}
					}
				}
				return dr;
			}

			public void reset() {
				if (!(this.it instanceof EmptyDbIterator)) {
					close();
					cursor = findIt.iterator();
					it = cursor;
				}
			}
		};
	}

	public IEditor getEditor(String db, String tableName, String[] keys) {
		return getEditor(db, tableName, keys, null);
	}

	public IEditor getEditor(final String db, final String tableName, final String[] keys, final String[] indexes) {
		return new IEditor() {
			private MongoCollection<Document> table;
			{
				table = getTable(db, tableName);
				if (indexes != null && indexes.length > 0) {
					for (String index : indexes) {
						table.createIndex(new BasicDBObject(index, Integer.valueOf(1)));
					}
				}
			}

			@Override
			public void addRecord(DataRecord dr) {
				addRecord(dr, false);
			}

			private Document getObject(DataRecord dr) {
				Document ob = new Document();
				for (String f : dr.getAllFields()) {
					Object obj = dr.getFieldObject(f);
					if (obj != null) {
						ob.put(f, obj);
					}
				}
				ob.put("_id", getKey(dr));

				return ob;
			}

			private String getKey(DataRecord dr) {
				List<String> list = new ArrayList<>();
				for (String key : keys) {
					list.add(dr.getFieldValue(key));
				}
				String key = StringUtil.getStringFromStrings(list, "어");
				return key.substring(0, Math.min(254, key.length()));
			}

			@Override
			public void addRecord(DataRecord dr, boolean override) {
				Document ob = getObject(dr);
				if (override) {
					UpdateOptions options = new UpdateOptions();
					options.upsert(true);
					this.table.replaceOne(eq("_id", getKey(dr)), ob, options);
				} else {
					this.table.insertOne(ob);
				}
			}

			@Override
			public void addRecords(List<DataRecord> drs) {
				addRecords(drs, false);
			}

			@Override
			public void addRecords(List<DataRecord> drs, boolean override) {
				if (override) {
					UpdateOptions options = new UpdateOptions();
					options.upsert(true);
					for (DataRecord dr : drs) {
						this.table.replaceOne(eq("_id", getKey(dr)), getObject(dr), options);
					}
				} else {
					List<Document> docs = new ArrayList<>();
					for (DataRecord dr : drs) {
						docs.add(getObject(dr));
					}
					this.table.insertMany(docs);
				}
			}

			@Override
			public void begineTransaction() {
			}

			@Override
			public void commit() {
			}

			@Override
			public boolean containsRecord(DataRecord dr) {
				return (this.table.find(eq("_id", getKey(dr))).iterator().hasNext());
			}

			@Override
			public void deleteRecord(DataRecord dr) {
				this.table.deleteMany(eq("_id", getKey(dr)));
			}

			@Override
			public void deleteRecords(List<DataRecord> drs) {
				for (DataRecord dr : drs) {
					deleteRecord(dr);
				}

			}

			@Override
			public void processRecord(DataRecord dr) {
			}

			@Override
			public void updateRecord(DataRecord dr) {
				addRecord(dr, true);
			}

			@Override
			public void execute(String sql) {
			}
		};
	}
}
