package org.asura.data.sql;

public class SQLTable implements ISQL {
	private static final long serialVersionUID = 1954729741879429984L;
	private FieldsSQL fields;

	public SQLTable() {
		this.fields = new FieldsSQL();
	}

	public void addTable(String table) {
		this.fields.addField(table);
	}

	public String getTable(int index) {
		return ((String[]) this.fields.getFields().toArray(new String[0]))[index];
	}

	public int size() {
		return this.fields.size();
	}

	public String getSQLString(ISQL.DBType type) {
		if (this.fields.size() == 0) {
			return "";
		}
		return " " + this.fields.getSQLString(type) + " ";
	}
}
