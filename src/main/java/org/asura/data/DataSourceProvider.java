package org.asura.data;

import java.util.List;

import org.asura.core.data.DataRecord;
import org.asura.core.exception.DataException;
import org.asura.core.util.cache.SimpleCache;
import org.asura.data.mongo.MongoConnection;
import org.asura.data.mongo.MongoHandler;
import org.asura.data.mysql.ConnectionInformation;
import org.asura.data.mysql.MysqlHandler;
import org.asura.data.oracle.OracleHandler;
import org.asura.data.sql.SelectSQL;

public class DataSourceProvider {
	private static SimpleCache<String, Object> cache = new SimpleCache<>(1000);
	private static final String MYSQL = "mysql";
	private static final String MONGO = "mongo";
	private static final String ORACLE = "oracle";

	public static MysqlHandler getMysqlHandler(String key) throws DataException {
		String cKey = MYSQL + key;
		if (cache.get(key)==null) {
			try {
				String config = findDataSource(MYSQL, key);
				ConnectionInformation ci = ConnectionInformation.fromXml(config);

				cache.cache(cKey, new MysqlHandler(ci));
			} catch (Exception e) {
				throw new DataException("mysql datasource " + key + " is not configed correctly.");
			}
		}

		return ((MysqlHandler) cache.get(cKey));
	}

	public static OracleHandler getOracleHandler(String key) throws DataException {
		String cKey = ORACLE + key;
		if (cache.get(key)==null) {
			try {
				String config = findDataSource(ORACLE, key);
				ConnectionInformation ci = ConnectionInformation.fromXml(config);

				cache.cache(cKey, new OracleHandler(ci));
			} catch (Exception e) {
				throw new DataException("mysql datasource " + key + " is not configed correctly.");
			}
		}

		return ((OracleHandler) cache.get(cKey));
	}

	public static MongoHandler getMongoHandler(String key) throws DataException {
		String cKey = (new StringBuilder(MONGO)).append(key).toString();
		if (cache.get(key)==null)
			try {
				String config = findDataSource(MONGO, key);
				if (config.contains("<") && config.contains(">"))
					cache.cache(cKey, new MongoHandler(MongoConnection.fromXml(config)));
				else
					cache.cache(cKey, new MongoHandler(config));
			} catch (Exception e) {
				throw new DataException((new StringBuilder("mongo datasource ")).append(key)
						.append(" is not configed correctly.").toString());
			}
		return (MongoHandler) cache.get(cKey);

	}

	private static String findDataSource(String type, String key) {
		SelectSQL sql = new SelectSQL("datasource");
		sql.addWhereCondition("type", type);
		sql.addWhereCondition("key", key);
		List<DataRecord> list = new MysqlHandler(ConnectionInformation.fromClassLoaderFile("datasource.xml")).selectList(sql);
		if (list.size() > 0) {
			return ((DataRecord) list.get(0)).getFieldValue("config");
		}

		return "";
	}
}
