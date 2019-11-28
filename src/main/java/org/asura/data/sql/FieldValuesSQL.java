package org.asura.data.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.asura.core.util.StringUtil;

public class FieldValuesSQL implements ISQL, Serializable {
	private static final long serialVersionUID = 4890847426921847082L;
	private LinkedHashSet<ISQL> fieldMap;
	private String spliter = ", ";

	public FieldValuesSQL() {
		this.fieldMap = new LinkedHashSet<>();
	}

	public FieldValuesSQL(String spliter) {
		this.fieldMap = new LinkedHashSet<>();
		this.spliter = spliter;
	}

	public void addFieldValue(String field, String value) {
		this.fieldMap.add(new SQLCondition(field, value));
	}

	public void addSQLCondition(ISQL condition) {
		this.fieldMap.add(condition);
	}

	public LinkedHashSet<ISQL> getFieldMap() {
		return this.fieldMap;
	}

	public int size() {
		return this.fieldMap.size();
	}

	public String getSQLString(ISQL.DBType type) {
		if (this.fieldMap.size() == 0) {
			return "";
		}
		List<String> list = new ArrayList<>();
		for (ISQL con : this.fieldMap) {
			String sql = con.getSQLString(type);
			if (!(StringUtil.isNullOrEmpty(sql))) {
				list.add(con.getSQLString(type));
			}
		}
		return " " + StringUtil.getStringFromStrings((String[]) list.toArray(new String[0]), this.spliter) + " ";
	}
}
