package org.asura.data.log;

import java.util.Date;

import org.asura.core.util.DateUtil;
import org.junit.Test;

public class TestMongoLogger {

	@Test
	public void test() {
		System.out.println(DateUtil.getDateString(new Date()));
		MLog logger=MongoLogFactory.getLogger("abctask@asdadsf");
		logger.info("first one");
		logger.error("exception occurred", new NullPointerException("hahahaha"));
		logger.info("second one");
	}

}
