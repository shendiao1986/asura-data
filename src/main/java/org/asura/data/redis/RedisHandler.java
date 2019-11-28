package org.asura.data.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.asura.core.util.cache.SimpleCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisHandler implements RedisOperation {

	private static final Logger logger = LoggerFactory.getLogger(RedisHandler.class);

	private RedisConnection connection;

	private static SimpleCache<String, JedisPool> redisPoolCache = new SimpleCache<>(10000);

	public RedisHandler(RedisConnection con) {
		this.connection = con;
	}

	public RedisHandler() {
		this.connection = RedisConnection.fromConfigFile();
	}

	private synchronized Jedis getJedis() {
		if (redisPoolCache.get(this.connection.getHost() + ":" + this.connection.getPort())==null) {
			logger.info("begin to initialize jedis pool " + this.connection);

			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxIdle(this.connection.getMaxIdle());
			config.setMaxTotal(this.connection.getMaxTotal());
			config.setMaxWaitMillis(this.connection.getMaxWaitMillis());
			config.setTestOnBorrow(this.connection.isTestOnBorrow());

			JedisPool pool = new JedisPool(config, this.connection.getHost(), this.connection.getPort(),
					this.connection.getTimeOut(), this.connection.getPassword());

			redisPoolCache.cache(this.connection.getHost() + ":" + this.connection.getPort(), pool);

			logger.info("end to initialize jedis pool " + this.connection);
		}

		return redisPoolCache.get(this.connection.getHost() + ":" + this.connection.getPort()).getResource();
	}

	public void close() {
		if (redisPoolCache.get(this.connection.getHost() + ":" + this.connection.getPort())!=null) {
			redisPoolCache.get(this.connection.getHost() + ":" + this.connection.getPort()).destroy();
			redisPoolCache.remove(this.connection.getHost() + ":" + this.connection.getPort());
		}
	}

	@Override
	public String set(String key, String value) {
		try (Jedis jedis = getJedis()) {
			return jedis.set(key, value);
		}
	}

	@Override
	public Long set(String key, String value, int seconds) {
		try (Jedis jedis = getJedis()) {
			jedis.set(key, value);
			return jedis.expire(key, seconds);
		}
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.hgetAll(key);
		}
	}

	@Override
	public String get(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.get(key);
		}
	}

	@Override
	public Long delKey(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.del(key);
		}
	}

	@Override
	public boolean exist(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.exists(key);
		}
	}

	@Override
	public Long increment(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.incr(key);
		}
	}

	@Override
	public Long incrementBy(String key, long value) {
		try (Jedis jedis = getJedis()) {
			return jedis.incrBy(key, value);
		}
	}

	@Override
	public Long zadd(String key, Map<String, Double> scoreMembers) {
		try (Jedis jedis = getJedis()) {
			return jedis.zadd(key, scoreMembers);
		}
	}

	@Override
	public Set<String> zrange(String key, long start, long end) {
		try (Jedis jedis = getJedis()) {
			return jedis.zrange(key, start, end);
		}
	}

	@Override
	public Long expire(String key, int seconds) {
		try (Jedis jedis = getJedis()) {
			return jedis.expire(key, seconds);
		}
	}

	@Override
	public Long zcard(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.zcard(key);
		}
	}

	@Override
	public Long zrem(String key, String... members) {
		try (Jedis jedis = getJedis()) {
			return jedis.zrem(key, members);
		}
	}

	@Override
	public String hmset(String key, Map<String, String> values) {
		try (Jedis jedis = getJedis()) {
			return jedis.hmset(key, values);
		}
	}

	@Override
	public String hget(String key, String field) {
		try (Jedis jedis = getJedis()) {
			return jedis.hget(key, field);
		}
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		try (Jedis jedis = getJedis()) {
			return jedis.hmget(key, fields);
		}
	}

	@Override
	public boolean hexists(String key, String field) {
		try (Jedis jedis = getJedis()) {
			return jedis.hexists(key, field);
		}
	}

	@Override
	public long hset(String key, String field, String value) {
		try (Jedis jedis = getJedis()) {
			return jedis.hset(key, field, value);
		}
	}

	@Override
	public long hdel(String key, String... field) {
		try (Jedis jedis = getJedis()) {
			return jedis.hdel(key, field);
		}
	}

	@Override
	public String rpop(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.rpop(key);
		}
	}

	@Override
	public long llen(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.llen(key);
		}
	}

	@Override
	public long lpush(String key, String value) {
		try (Jedis jedis = getJedis()) {
			return jedis.lpush(key, value);
		}
	}

	@Override
	public List<String> brPop(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.brpop(Integer.MAX_VALUE, key);
		}
	}

	@Override
	public List<String> brPop(String key, int seconds) {
		try (Jedis jedis = getJedis()) {
			return jedis.brpop(seconds, key);
		}
	}

	@Override
	public Jedis jedis() {
		return getJedis();
	}

	@Override
	public Set<String> hkeys(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.hkeys(key);
		}
	}

	@Override
	public Set<String> keys(String pattern) {
		try (Jedis jedis = getJedis()) {
			return jedis.keys(pattern);
		}
	}

	@Override
	public long hlen(String key) {
		try (Jedis jedis = getJedis()) {
			return jedis.hlen(key);
		}
	}

}
