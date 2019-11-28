package org.asura.data.sql;

import java.io.Serializable;

public interface ISQL extends Serializable {
	public abstract String getSQLString(DBType dbType);

	public static enum DBType {
		mysql, oracle;
	}
}