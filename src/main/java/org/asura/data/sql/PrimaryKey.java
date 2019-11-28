package org.asura.data.sql;

public class PrimaryKey implements ISQL {
	private static final long serialVersionUID = 242242553394550932L;
	private FieldsSQL keys;

	public PrimaryKey() {
		this.keys = new FieldsSQL();
	}

	public PrimaryKey(String[] keys) {
		this.keys = new FieldsSQL();
		this.keys.addFields(keys);
	}

	public void addKey(String key) {
		this.keys.addField(key);
	}

	public void addKeys(String[] keys) {
		this.keys.addFields(keys);
	}

	public FieldsSQL getKeys() {
		return this.keys;
	}

	public void setKeys(FieldsSQL keys) {
		this.keys = keys;
	}

	public String getSQLString(ISQL.DBType type) {
		return " PRIMARY KEY (" + this.keys.getSQLString(type) + ")" + " ";
	}
}
