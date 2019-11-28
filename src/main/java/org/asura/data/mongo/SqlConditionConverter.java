package org.asura.data.mongo;

import java.util.regex.Pattern;

import org.asura.core.util.math.NumberUtil;
import org.asura.data.sql.ISQL;
import org.asura.data.sql.SQLCondition;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;

public class SqlConditionConverter implements IMongoConverter {
	public Bson convert(ISQL sql) {
		SQLCondition con = (SQLCondition) sql;

		Object o = getValue(con);

		Bson ob = new BasicDBObject(con.getField(), o);

		if (con.getCondition().equals("="))
			ob = new BasicDBObject(con.getField(), o);
		else if (con.getCondition().equals(">"))
			ob = new BasicDBObject(con.getField(), new BasicDBObject("$gt", o));
		else if (con.getCondition().equals(">="))
			ob = new BasicDBObject(con.getField(), new BasicDBObject("$gte", o));
		else if (con.getCondition().equals("<"))
			ob = new BasicDBObject(con.getField(), new BasicDBObject("$lt", o));
		else if (con.getCondition().equals("<="))
			ob = new BasicDBObject(con.getField(), new BasicDBObject("$lte", o));
		else if (con.getCondition().equals("<>"))
			ob = new BasicDBObject(con.getField(), new BasicDBObject("$ne", o));
		else if (con.getCondition().equals("like")) {
			ob = new BasicDBObject(con.getField(), Pattern.compile(con.getValue(), 2));
		}

		return ob;
	}

	public boolean canConvert(ISQL sql) {
		return sql instanceof SQLCondition;
	}

	private Object getValue(SQLCondition con) {
		if (con.isNumber()) {
			return Double.valueOf(NumberUtil.getDouble(con.getValue()));
		}

		return con.getValue();
	}
}
