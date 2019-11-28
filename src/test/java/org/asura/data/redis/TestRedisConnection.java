package org.asura.data.redis;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class TestRedisConnection {

	@Test
	public void test() {
		RedisHandler handler = new RedisHandler();
		handler.hset("haha", "name", "000");
		Jedis jedis = handler.jedis();
		Transaction tx = jedis.multi();
		tx.del("haha");
		tx.hset("haha", "name", "111");
		tx.del("haha");
		tx.hset("haha", "name", "222");
		tx.del("haha");
		tx.hset("haha", "name", "333");
		tx.del("haha");
		tx.hset("haha", "name", "444");
		tx.del("haha");
		tx.hset("haha", "name", "555");
		tx.del("haha");
		tx.hset("haha", "name", "666");
		tx.del("haha");
		tx.hset("haha", "name", "777");
		tx.del("haha");
		tx.hset("haha", "name", "999");
		tx.del("haha");
		tx.hset("haha", "name", "777");
		tx.exec();
		jedis.close();
		handler.close();
	}

}
