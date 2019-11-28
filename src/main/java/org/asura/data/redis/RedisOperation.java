package org.asura.data.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;

public interface RedisOperation {

	public String set(String key, String value);
	
	public Long set(String key, String value, int seconds);

	public Map<String, String> hgetAll(String key);

	public String get(String key);

	public Long delKey(String key);

	public boolean exist(String key);

	public Long increment(String key);

	public Long incrementBy(String key, long value);

	public Long zadd(String key, Map<String, Double> scoreMembers);

	public Set<String> zrange(String key, long start, long end);

	public Long expire(String key, int seconds);

	public Long zcard(String key);

	public Long zrem(String key, String... members);

	public String hmset(String key, Map<String, String> values);

	public String hget(String key, String field);

	public List<String> hmget(String key, String... fields);

	public boolean hexists(String key, String field);

	public long hset(String key, String field, String value);

	public long hdel(String key, String... field);

	public String rpop(String key);

	public long llen(String key);

	public long lpush(String key, String value);

	public List<String> brPop(String key);

	public List<String> brPop(String key, int seconds);

	public void close();

	public Jedis jedis();

	public Set<String> hkeys(String key);

	public Set<String> keys(String pattern);

	public long hlen(String key);
}
