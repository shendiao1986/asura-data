package org.asura.data.redis;

import org.junit.Test;

public class TestRedisConnection2 {

	@Test
	public void test() {
		RedisHandler handler = new RedisHandler();
		System.out.println(handler.hget("midas.appkeys", "com.kugou.fanxing"));
		handler.jedis().keys("");
	}
}
