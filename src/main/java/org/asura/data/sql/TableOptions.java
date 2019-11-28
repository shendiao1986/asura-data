package org.asura.data.sql;

public class TableOptions implements ISQL {
	private static final long serialVersionUID = 647157794159301082L;
	public static final String INNODB = "InnoDB";
	public static final String MYISAM = "MyISAM";
	public static final String UTF8 = "utf8";
	private String engine;
	private String charset;

	public TableOptions() {
		this.engine = "InnoDB";
		this.charset = "utf8";
	}

	public TableOptions(String engine, String charset) {
		this.engine = engine;
		this.charset = charset;
	}

	public String getEngine() {
		return this.engine;
	}

	public void setEngine(String engine) {
		this.engine = engine;
	}

	public String getCharset() {
		return this.charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getSQLString(ISQL.DBType type) {
		return " ENGINE=" + this.engine + " " + "DEFAULT" + " " + "CHARSET=" + this.charset + " ";
	}
}
