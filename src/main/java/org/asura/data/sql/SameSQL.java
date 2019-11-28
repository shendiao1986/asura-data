package org.asura.data.sql;

public class SameSQL implements ISQL {
	private static final long serialVersionUID = -3479331882891751395L;
	private String sql;

	public SameSQL(String sql) {
		this.sql = sql;
	}

	public String getSql() {
		return this.sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getSQLString(ISQL.DBType type) {
		return this.sql;
	}
}
