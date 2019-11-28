package org.asura.data.sql;

import org.asura.core.data.DataRecord;

public class UpdateSQL implements ISQL {
	private static final long serialVersionUID = 3314259832917685587L;
	private SQLTable tables;
	private FieldValuesSQL fieldValues;
	private WhereSQL where;

	public UpdateSQL() {
		this.tables = new SQLTable();
		this.fieldValues = new FieldValuesSQL();
		this.where = new WhereSQL();
	}

	public void addTable(String table) {
		this.tables.addTable(table);
	}

	public void addWhereCondition(String field, String value) {
		this.where.addCondition(field, value);
	}

	public void addFieldValues(DataRecord dr) {
		for (String field : dr.getAllFields())
			this.fieldValues.addFieldValue(field, dr.getFieldValue(field));
	}

	public void addFiendValue(String field, String value) {
		this.fieldValues.addFieldValue(field, value);
	}

	public String getSQLString(ISQL.DBType type) {
		StringBuffer sb = new StringBuffer();
		sb.append("update ");
		sb.append(this.tables.getSQLString(type));
		sb.append(" set ");
		sb.append(this.fieldValues.getSQLString(type));
		sb.append(this.where.getSQLString(type));

		return SQLUtils.removeMultiBlank(sb.toString().trim());
	}
}
