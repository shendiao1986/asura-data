package org.asura.data.sql;

public class SQLCondition implements ISQL {
	private static final long serialVersionUID = -7914377517128087257L;
	private String field;
	private String value;
	private boolean negative = false;
	private String condition = "=";
	private boolean number;

	public SQLCondition() {
	}

	public boolean isNegative() {
		return this.negative;
	}

	public void setNegative(boolean negative) {
		this.negative = negative;
	}

	public boolean isNumber() {
		return this.number;
	}

	public void setNumber(boolean number) {
		this.number = number;
	}

	public SQLCondition(String field, String value) {
		this.field = field;
		this.value = value;
	}

	public SQLCondition(String field, String value, String condition, boolean number) {
		this.field = field;
		this.value = value;
		this.condition = condition;
		this.number = number;
	}

	public SQLCondition(String field, String value, String condition) {
		this.field = field;
		this.value = value;
		this.condition = condition;
		this.number = false;
	}

	public String getField() {
		return this.field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getValue() {
		return this.value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getCondition() {
		return this.condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getSQLString(ISQL.DBType type) {
		if ((this.value != null) && (this.field != null)) {
			String v = " '" + SQLUtils.getStandardSQLValue(this.value) + "' ";
			if (this.number) {
				v = " " + SQLUtils.getStandardSQLValue(this.value) + " ";
			}
			if (type == ISQL.DBType.mysql) {
				if (this.negative) {
					return "!(`" + this.field + "`" + this.condition + v + ")";
				}
				return "`" + this.field + "`" + this.condition + v;
			}

			if (this.negative) {
				return "!(" + this.field + this.condition + v + ")";
			}
			return this.field + this.condition + v;
		}

		return "";
	}

	public int hashCode() {
		int result = 1;
		result = 31 * result + ((this.condition == null) ? 0 : this.condition.hashCode());
		result = 31 * result + ((this.field == null) ? 0 : this.field.hashCode());
		result = 31 * result + ((this.value == null) ? 0 : this.value.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (super.getClass() != obj.getClass())
			return false;
		SQLCondition other = (SQLCondition) obj;
		if (this.condition == null)
			if (other.condition != null)
				return false;
			else if (!(this.condition.equals(other.condition)))
				return false;
		if (this.field == null)
			if (other.field != null)
				return false;
			else if (!(this.field.equals(other.field)))
				return false;
		if (this.value == null)
			if (other.value != null)
				return false;
			else if (!(this.value.equals(other.value)))
				return false;
		return true;
	}
}