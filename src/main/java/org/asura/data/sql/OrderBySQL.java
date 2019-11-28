package org.asura.data.sql;

public class OrderBySQL implements ISQL {
	private static final long serialVersionUID = 1690455484006921669L;
	private FieldsSQL fields;

	public OrderBySQL() {
		this.fields = new FieldsSQL();
	}

	public void addField(String field) {
		this.fields.addField(field);
	}

	public void addDescField(String field) {
		this.fields.addField(field + " desc ");
	}

	public FieldsSQL getFields() {
		return this.fields;
	}

	public String getSQLString(ISQL.DBType type) {
		if (this.fields.size() == 0) {
			return "";
		}
		String field = this.fields.getSQLString(type);

		return " order by " + field + " ";
	}
}
