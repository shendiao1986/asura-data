package org.asura.data.sql;

import java.io.Serializable;

public class GroupBySQL implements ISQL, Serializable {
	private static final long serialVersionUID = 1558446713198103334L;
	private FieldsSQL fields;

	public GroupBySQL() {
		this.fields = new FieldsSQL();
	}

	public void addField(String field) {
		this.fields.addField(field);
	}

	public FieldsSQL getFields() {
		return this.fields;
	}

	public String getSQLString(ISQL.DBType type) {
		if (this.fields.size() == 0) {
			return "";
		}
		return " group by " + this.fields.getSQLString(type) + " ";
	}
}
