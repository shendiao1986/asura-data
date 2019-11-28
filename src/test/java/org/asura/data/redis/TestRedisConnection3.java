package org.asura.data.redis;

import org.junit.Test;

public class TestRedisConnection3 {

	@Test
	public void test() throws InterruptedException {
		RedisHandler handler = new RedisHandler();
		handler.set("aaaa", "bbb");
		System.out.println(handler.get("aaaa"));

	}
}
