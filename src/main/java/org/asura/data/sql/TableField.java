package org.asura.data.sql;

public class TableField implements ISQL {
	private static final long serialVersionUID = 6780927375233634166L;
	private String fieldName;
	private String fieldType;
	private boolean canNull;
	private String defaultValue = "";
	private boolean autoIncrement;

	public TableField() {
	}

	public TableField(String fieldName, String fieldType, boolean canNull, String defaultValue, boolean autoIncrement) {
		this.fieldName = fieldName;
		this.fieldType = fieldType;
		this.canNull = canNull;
		this.defaultValue = defaultValue;
		this.autoIncrement = autoIncrement;
	}

	public boolean isAutoIncrement() {
		return this.autoIncrement;
	}

	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}

	public String getFieldName() {
		return this.fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getFieldType() {
		return this.fieldType;
	}

	public void setFieldType(String fieldType) {
		this.fieldType = fieldType;
	}

	public boolean isCanNull() {
		return this.canNull;
	}

	public void setCanNull(boolean canNull) {
		this.canNull = canNull;
	}

	public String getDefaultValue() {
		return this.defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public static long getSerialVersionUID() {
		return 6780927375233634166L;
	}

	public String getSQLString(ISQL.DBType type) {
		if (!(this.autoIncrement)) {
			return "`" + this.fieldName + "`" + " " + this.fieldType + " " + ((this.canNull) ? "" : " NOT NULL ")
					+ " DEFAULT '" + this.defaultValue + "'";
		}
		return "`" + this.fieldName + "`" + " " + this.fieldType + " " + ((this.canNull) ? "" : " NOT NULL ")
				+ " AUTO_INCREMENT ";
	}
}
