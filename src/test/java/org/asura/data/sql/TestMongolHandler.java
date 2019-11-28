package org.asura.data.sql;

import java.util.Date;
import java.util.List;

import org.asura.core.data.DataRecord;
import org.asura.core.data.IEditor;
import org.asura.core.util.DateUtil;
import org.asura.data.mongo.MongoHandler;
import org.asura.data.sql.BooleanCondition.BooleanClause;
import org.junit.Test;

public class TestMongolHandler {

	@Test
	public void test() {
		//new MongoHandler("127.0.0.1").deleteTable("test", "ddd");

		IEditor editor = new MongoHandler("127.0.0.1").getEditor("test", "ddd", new String[] { "a", "b" },
				new String[] { "a", "b", "c", "d" });

		for (int i = 0; i < 8; i++) {
			DataRecord record = new DataRecord();
			record.AddField("a", i);
			record.AddField("b", i);
			record.AddField("c", i);
			record.AddField("d", i);
			record.AddField("update", DateUtil.getDateTimeString(new Date()));
			editor.addRecord(record, true);
		}

		System.out.println(new MongoHandler("127.0.0.1").getCount("test", "ddd"));

		SelectSQL sql = new SelectSQL("ddd");

		BooleanCondition condition = new BooleanCondition();

		condition.addCondition(new SQLCondition("a", "3", "=", true), BooleanClause.Should);

		condition.addCondition(new SQLCondition("b", "5", "<", true), BooleanClause.Should);
		sql.addBooleanCondition(condition);

		sql.setLimitCount(3);
		sql.setLimitStart(1);

		sql.addOrderByField("a", false);

		System.out.println(new MongoHandler("127.0.0.1").getCount("test", sql));

		List<DataRecord> list = new MongoHandler("127.0.0.1").selectList("test", sql);
		for (DataRecord l : list) {
			System.out.println(l);
		}

		DataRecord record = new DataRecord();
		record.AddField("a", 11);
		record.AddField("b", 1);

		System.out.println(editor.containsRecord(record));

	}

}
