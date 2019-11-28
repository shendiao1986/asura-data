package org.asura.data.mongo;

import org.asura.core.util.cache.SimpleCache;

public class MongoService {
	private static SimpleCache<String, MongoHandler> handler = new SimpleCache<>(1000);

	public static void addRecord(String host, String db, String table, String[] keys, String[] index, String sql) {
		if (handler.get(host) == null) {
			handler.cache(host, new MongoHandler(host));
		}

	}

	public static void execute(String host, String db, String table, String sql) {
	}
}
