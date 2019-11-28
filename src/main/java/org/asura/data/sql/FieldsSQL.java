package org.asura.data.sql;

import java.io.Serializable;
import java.util.LinkedHashSet;

import org.asura.core.util.StringUtil;

public class FieldsSQL implements ISQL, Serializable {
	private static final long serialVersionUID = -7334138548956597158L;
	private LinkedHashSet<String> fields;

	public FieldsSQL() {
		this.fields = new LinkedHashSet<>();
	}

	public void addField(String field) {
		this.fields.add(field);
	}

	public void addFields(String[] fields) {
		for (String field : fields)
			this.fields.add(field);
	}

	public void clear() {
		this.fields.clear();
	}

	public LinkedHashSet<String> getFields() {
		return this.fields;
	}

	public int size() {
		return this.fields.size();
	}

	public String getSQLString(ISQL.DBType type) {
		if (this.fields.size() == 0) {
			return "";
		}
		return " " + StringUtil.getStringFromStrings((String[]) this.fields.toArray(new String[0]), ", ") + " ";
	}
}
