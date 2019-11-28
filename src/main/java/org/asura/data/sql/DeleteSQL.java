package org.asura.data.sql;

public class DeleteSQL implements ISQL {
	private static final long serialVersionUID = -2945644236668850194L;
	private SQLTable tables;
	private WhereSQL where;

	public DeleteSQL() {
		this.tables = new SQLTable();
		this.where = new WhereSQL();
	}

	public void addTable(String table) {
		this.tables.addTable(table);
	}

	public void addWhereCondition(String field, String value) {
		this.where.addCondition(field, value);
	}

	public void addWhereCondition(SQLCondition condition) {
		this.where.addCondition(condition);
	}

	public SQLTable getTables() {
		return this.tables;
	}

	public WhereSQL getWhere() {
		return this.where;
	}

	public String getSQLString(ISQL.DBType type) {
		StringBuffer sb = new StringBuffer();
		sb.append("delete from ");
		sb.append(this.tables.getSQLString(type));
		sb.append(this.where.getSQLString(type));

		return SQLUtils.removeMultiBlank(sb.toString());
	}
}
