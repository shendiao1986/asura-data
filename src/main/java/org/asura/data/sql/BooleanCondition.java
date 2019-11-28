package org.asura.data.sql;

import java.util.ArrayList;

public class BooleanCondition implements ISQL {
	private static final long serialVersionUID = 5167210794489313487L;
	private ArrayList<ISQL> andList;
	private ArrayList<ISQL> orList;

	public BooleanCondition() {
		this.andList = new ArrayList<>();
		this.orList = new ArrayList<>();
	}

	public void addCondition(ISQL sql, BooleanClause clause) {
		switch (clause) {
		case Should:
			this.orList.add(sql);
			break;
		case Must:
			this.andList.add(sql);
			break;
		default:
			this.andList.add(sql);
		}
	}

	public ArrayList<ISQL> getAndList() {
		return this.andList;
	}

	public ArrayList<ISQL> getOrList() {
		return this.orList;
	}

	public String getSQLString(ISQL.DBType type) {
		if ((this.orList.size() > 0) || (this.andList.size() > 0)) {
			String sb = "";
			if (this.andList.size() > 0) {
				FieldValuesSQL fv = new FieldValuesSQL(" and ");
				for (ISQL sql : this.andList) {
					fv.addSQLCondition(sql);
				}

				sb = sb + fv.getSQLString(type);
			}

			if (this.orList.size() > 0) {
				FieldValuesSQL fv = new FieldValuesSQL(" or ");
				for (ISQL sql : this.orList) {
					fv.addSQLCondition(sql);
				}

				if (this.andList.size() > 0)
					sb = sb + " and (" + fv.getSQLString(type) + ")";
				else {
					sb = fv.getSQLString(type);
				}

			}

			return "(" + sb + ")";
		}

		return "";
	}

	public static enum BooleanClause {
		Must, Should;
	}
}
