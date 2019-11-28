package org.asura.core.util;

import org.asura.core.util.cache.RedisCache;
import org.asura.data.redis.RedisConnection;
import org.asura.data.redis.RedisHandler;
import org.junit.Test;

public class TestRedisCache {

	@Test
	public void test() {
		RedisConnection con=new RedisConnection();
		con.setHost("127.0.0.1");
		con.setPort(6379);
		con.setTimeOut(1000);
		con.setMaxTotal(100);
		con.setMaxIdle(10);
		con.setMaxWaitMillis(100);
		con.setTestOnBorrow(true);
		
		RedisHandler handler=RedisUtil.getRedisHandler(con);
		RedisCache cache=new RedisCache(handler, "cachetest");
		cache.cache("a", "a");
		cache.cache("b", "a");
		cache.cache("c", "a");
		cache.cache("1", "1");
		cache.cache("2", "2");
		cache.cache("3", "3");
		
		System.out.println(cache.size());
		
		cache.clear();
		
		System.out.println(cache.size());
	}

}
