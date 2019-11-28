package org.asura.data.sql;

public class LimitSQL implements ISQL {
	private static final long serialVersionUID = 8073415816067491934L;
	private int start;
	private int count;

	public LimitSQL() {
		this.start = 0;
		this.count = 0;
	}

	public LimitSQL(int count) {
		this.start = 0;
		this.count = count;
	}

	public LimitSQL(int start, int count) {
		this.start = start;
		this.count = count;
	}

	public int getStart() {
		return this.start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getCount() {
		return this.count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getSQLString(ISQL.DBType type) {
		if ((this.count == 0) && (this.start == 0))
			return "";
		if ((this.count != 0) && (this.start == 0)) {
			return " limit " + this.count;
		}
		return " limit " + this.start + ", " + this.count;
	}
}
