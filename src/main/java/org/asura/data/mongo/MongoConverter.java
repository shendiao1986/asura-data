package org.asura.data.mongo;

import java.util.ArrayList;
import java.util.List;

import org.asura.data.sql.ISQL;
import org.bson.conversions.Bson;

public class MongoConverter {
	private static List<IMongoConverter> converters = new ArrayList<>();

	static {
		converters.add(new ClauseConditionConverter());
		converters.add(new SqlConditionConverter());
	}

	public static Bson convert(ISQL sql) {
		for (IMongoConverter converter : converters) {
			if (converter.canConvert(sql)) {
				return converter.convert(sql);
			}
		}

		return null;
	}
}
