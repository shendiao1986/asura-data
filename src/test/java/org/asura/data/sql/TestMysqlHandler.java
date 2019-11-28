package org.asura.data.sql;

import org.asura.core.data.DataRecord;
import org.asura.core.util.math.NumberUtil;
import org.asura.data.mysql.MysqlHandler;
import org.junit.Test;

public class TestMysqlHandler {

	@Test
	public void test() {
		DataRecord condition=new DataRecord();
		condition.AddField("taskdate", "2016-07-13");
		condition.AddField("name", "download-log-parse");
		DataRecord result=new MysqlHandler().getDataRecordByCondition("task_data_status", condition);
		System.out.println(result);
		if(result!=null){
			System.out.println(NumberUtil.getInt(result.getFieldValue("taskstatus")));
		}
	}

}
