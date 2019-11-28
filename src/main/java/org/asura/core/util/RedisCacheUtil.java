package org.asura.core.util;

import org.asura.core.util.cache.RedisCache;
import org.asura.data.redis.RedisHandler;

public class RedisCacheUtil {

	public static RedisCache getRedisCache(RedisHandler handler, String cacheKey) {
		return new RedisCache(handler, cacheKey);
	}
	
}
