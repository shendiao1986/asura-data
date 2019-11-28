package org.asura.core.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.asura.data.redis.RedisConnection;
import org.asura.data.redis.RedisHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 对redis进行操作的工具类
 * 
 * @author shendiao
 *
 */
public class RedisUtil {

	private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);

	/**
	 * 获取RedisHandler，配置信息从classpath中的rediscon.xml中读取
	 * 
	 * @return RedisHandler对象
	 */
	public static RedisHandler getRedisHandler() {
		return new RedisHandler();
	}

	/**
	 * 获取RedisHandler，配置信息从<code>con</code>中获取
	 * 
	 * @return RedisHandler对象
	 */
	public static RedisHandler getRedisHandler(RedisConnection con) {
		return new RedisHandler(con);
	}

	/**
	 * 该方法用来同步数据<code>data</code>到key指定的hash数据结构中，可指定更新的batchSize，是否增量更新，appended=true表示增量更新，appended=false，表示全部用新的数据覆盖
	 * 
	 * @param handler RedisHandler
	 * @param key 指定的key
	 * @param data 需要同步的data
	 * @param batchSize 每个批次同步数据的大小
	 * @param appended true表示增量同步，false表示全量覆盖
	 */
	public static void syncHashKVs(RedisHandler handler, String key, Map<String, ?> data, int batchSize,
			boolean appended) {
		if (!appended) {
			int count = 0;
			Set<String> memFields = handler.hkeys(key);
			memFields.removeAll(data.keySet());
			logger.info("deprecated field size is " + memFields.size() + " for key " + key);
			List<String> subFields = new ArrayList<>();
			for (String field : memFields) {
				subFields.add(field);
				if (++count % batchSize == 0) {
					handler.hdel(key, subFields.toArray(new String[] {}));
					subFields.clear();
					logger.info("delete deprecated field size " + count);
				}
			}
			if (subFields.size() > 0) {
				handler.hdel(key, subFields.toArray(new String[] {}));
				subFields.clear();
				logger.info("delete deprecated field size " + count);
			}
		}

		Map<String, String> subMap = new HashMap<>();
		int count = 0;
		for (Entry<String, ?> entry : data.entrySet()) {
			subMap.put(entry.getKey(), JsonUtil.fromObject(entry.getValue()));
			if (++count % batchSize == 0) {
				handler.hmset(key, subMap);
				subMap.clear();
				logger.info("added field size " + count);
			}
		}
		if (subMap.size() > 0) {
			handler.hmset(key, subMap);
			subMap.clear();
			logger.info("added field size " + count);
		}
	}
}
