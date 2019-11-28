package org.asura.data.mongo;

import org.asura.data.sql.ISQL;
import org.bson.conversions.Bson;

public interface IMongoConverter {
	public Bson convert(ISQL paramISQL);

	public boolean canConvert(ISQL paramISQL);
}
