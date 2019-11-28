package org.asura.data.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.asura.core.data.DataRecord;
import org.asura.core.util.StringUtil;

public class InsertSQL implements ISQL, Serializable {
	private static final long serialVersionUID = -7951460546071669844L;
	private boolean ignore;
	private boolean override;
	private SQLTable tables;
	private FieldValuesSQL fieldValues;

	public InsertSQL() {
		this.tables = new SQLTable();
		this.fieldValues = new FieldValuesSQL();
		this.ignore = true;
	}

	public void addTable(String table) {
		this.tables.addTable(table);
	}

	public void addFieldValue(String field, String value) {
		this.fieldValues.addFieldValue(field, value);
	}

	public void addFieldValue(DataRecord dataRecord) {
		for (String field : dataRecord.getAllFields())
			this.fieldValues.addFieldValue(field, dataRecord.getFieldValue(field));
	}

	public boolean isIgnore() {
		return this.ignore;
	}

	public void setIgnore(boolean ignore) {
		this.ignore = ignore;
	}

	public boolean isOverride() {
		return this.override;
	}

	public void setOverride(boolean override) {
		this.override = override;
	}

	public String getSQLString(ISQL.DBType type) {
		StringBuffer sb = new StringBuffer();

		if (type == ISQL.DBType.mysql)
			if (this.ignore) {
				sb.append("insert ");
				sb.append(" ignore ");
			} else {
				sb.append("replace");
			}
		else {
			sb.append("insert ");
		}

		sb.append(" into ");
		sb.append(this.tables.getSQLString(type));
		if (type == ISQL.DBType.mysql) {
			sb.append(" set ");
			sb.append(this.fieldValues.getSQLString(type));
		} else {
			ISQL[] fields = (ISQL[]) this.fieldValues.getFieldMap().toArray(new ISQL[0]);
			if (fields.length > 0) {
				List<String> fList = new ArrayList<>();
				List<String> vList = new ArrayList<>();
				for (ISQL f : fields) {
					SQLCondition sc = (SQLCondition) f;
					fList.add("\"" + sc.getField() + "\"");
					vList.add(" '" + SQLUtils.getStandardSQLValue(sc.getValue()) + "' ");
				}

				sb.append(" (" + StringUtil.getStringFromStrings(fList, ",") + ") values("
						+ StringUtil.getStringFromStrings(vList, ",") + ") ");
			}
		}

		return SQLUtils.removeMultiBlank(sb.toString().trim());
	}
}
