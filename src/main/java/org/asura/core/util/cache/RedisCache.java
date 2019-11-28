package org.asura.core.util.cache;

import org.asura.core.exception.InvalidParamException;
import org.asura.core.util.StringUtil;
import org.asura.data.redis.RedisHandler;

public class RedisCache implements ICache<String, String> {

	private static final long serialVersionUID = -8238201237028935310L;

	private static final String SEPARATOR = "|";

	private RedisHandler handler;
	private String cacheKey;

	public RedisCache(RedisHandler handler, String cacheKey) {
		this.handler = handler;
		this.cacheKey = cacheKey;
		if (StringUtil.isNullOrEmpty(cacheKey)) {
			throw new InvalidParamException("cache key should not be null or empty");
		}
	}

	@Override
	public String get(String key) {
		return this.handler.get(this.cacheKey + SEPARATOR + key);
	}

	@Override
	public void cache(String key, String value) {
		this.handler.set(this.cacheKey + SEPARATOR + key, value);
	}

	@Override
	public void cache(String key, String value, int seconds) {
		this.handler.set(this.cacheKey + SEPARATOR + key, value, seconds);
	}

	@Override
	public void remove(String key) {
		this.handler.delKey(this.cacheKey + SEPARATOR + key);
	}

	@Override
	public int size() {
		return handler.keys(this.cacheKey + SEPARATOR + "*").size();
	}

	@Override
	public void clear() {
		for (String key : handler.keys(this.cacheKey + SEPARATOR + "*")) {
			handler.delKey(key);
		}
	}

}
