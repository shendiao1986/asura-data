package org.asura.data.mongo;

import org.asura.data.sql.BooleanCondition;
import org.asura.data.sql.ISQL;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class ClauseConditionConverter implements IMongoConverter {
	public boolean canConvert(ISQL sql) {
		return sql instanceof BooleanCondition;
	}

	public Bson convert(ISQL sql) {
		BooleanCondition bc = (BooleanCondition) sql;

		Bson ob = null;

		if (bc.getAndList().size() > 0) {
			BasicDBList list = new BasicDBList();
			for (ISQL con : bc.getAndList()) {
				Bson and = MongoConverter.convert(con);
				if (and != null) {
					list.add(and);
				}
			}

			if (list.size() > 0) {
				ob = new BasicDBObject("$and", list);
			}
		}

		if (bc.getOrList().size() > 0) {
			BasicDBList list = new BasicDBList();
			for (ISQL con : bc.getOrList()) {
				Bson or = MongoConverter.convert(con);
				if (or != null) {
					list.add(or);
				}
			}

			if (list.size() > 0) {
				if (ob == null) {
					ob = new BasicDBObject("$or", list);
				} else {
					BasicDBList newList = new BasicDBList();
					newList.add(ob);
					newList.add(new BasicDBObject("$or", list));

					ob = new BasicDBObject("$and", newList);
				}
			}
		}

		return ob;
	}
}
