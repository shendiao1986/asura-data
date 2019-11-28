package org.asura.data.sql;

import java.io.Serializable;

public class CreateTableSQL implements ISQL, Serializable {
	private static final long serialVersionUID = -2599858174701900906L;
	private String tableName;
	private FieldValuesSQL fields;
	private TableOptions options;

	public CreateTableSQL() {
		this.fields = new FieldValuesSQL();
		this.options = new TableOptions();
	}

	public CreateTableSQL(String tableName) {
		this.tableName = tableName;
		this.fields = new FieldValuesSQL();
		this.options = new TableOptions();
	}

	public void addField(TableField field) {
		this.fields.addSQLCondition(field);
	}

	public void setPrimaryKey(PrimaryKey key) {
		this.fields.addSQLCondition(key);
	}

	public String getTableName() {
		return this.tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public FieldValuesSQL getFields() {
		return this.fields;
	}

	public void setFields(FieldValuesSQL fields) {
		this.fields = fields;
	}

	public TableOptions getOptions() {
		return this.options;
	}

	public void setOptions(TableOptions options) {
		this.options = options;
	}

	public String getSQLString(ISQL.DBType type) {
		return SQLUtils.removeMultiBlank("CREATE TABLE `" + this.tableName + "` (" + this.fields.getSQLString(type)
				+ ")" + " " + this.options.getSQLString(type) + ";");
	}
}