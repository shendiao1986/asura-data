package org.asura.data.sql;

import java.io.Serializable;

import org.asura.core.util.SerializeUtil;
import org.asura.core.util.StringUtil;

public class SelectSQL implements ISQL, Serializable {
	private static final long serialVersionUID = 1005831042004398319L;
	private FieldsSQL fields;
	private SQLTable tables;
	private WhereSQL where;
	private OrderBySQL orderBy;
	private GroupBySQL groupBy;
	private LimitSQL limit;
	private String sql;

	public SelectSQL() {
		this.tables = new SQLTable();
		this.fields = new FieldsSQL();
		this.where = new WhereSQL();
		this.orderBy = new OrderBySQL();
		this.groupBy = new GroupBySQL();
		this.limit = new LimitSQL();
	}

	public SelectSQL(String table) {
		this();
		addTable(table);
	}

	public SelectSQL(String table, String[] fields) {
		this();
		addTable(table);
		addFields(fields);
	}

	public String getSql() {
		return this.sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public void addField(String field) {
		this.fields.addField(field);
	}

	public void addFields(String[] fields) {
		this.fields.addFields(fields);
	}

	public void addTable(String table) {
		this.tables.addTable(table);
	}

	public void addWhereCondition(String field, String value) {
		this.where.addCondition(field, value);
	}

	public void addWhereCondition(SQLCondition condition) {
		this.where.addCondition(condition);
	}

	public void addWhereCondition(ISQL condition) {
		this.where.addCondition(condition);
	}

	public void addBooleanCondition(BooleanCondition condition) {
		this.where.addCondition(condition);
	}

	public void addGroupByField(String field) {
		this.groupBy.addField(field);
	}

	public void addGroupByFields(String[] fields) {
		for (String field : fields)
			this.groupBy.addField(field);
	}

	public FieldsSQL getFields() {
		return this.fields;
	}

	public void setFields(FieldsSQL fields) {
		this.fields = fields;
	}

	public SQLTable getTables() {
		return this.tables;
	}

	public void setTables(SQLTable tables) {
		this.tables = tables;
	}

	public WhereSQL getWhere() {
		return this.where;
	}

	public void setWhere(WhereSQL where) {
		this.where = where;
	}

	public OrderBySQL getOrderBy() {
		return this.orderBy;
	}

	public void setOrderBy(OrderBySQL orderBy) {
		this.orderBy = orderBy;
	}

	public GroupBySQL getGroupBy() {
		return this.groupBy;
	}

	public void setGroupBy(GroupBySQL groupBy) {
		this.groupBy = groupBy;
	}

	public LimitSQL getLimit() {
		return this.limit;
	}

	public void setLimit(LimitSQL limit) {
		this.limit = limit;
	}

	public void addOrderByField(String field, boolean desc) {
		if (!(desc))
			this.orderBy.addField(field);
		else
			this.orderBy.addDescField(field);
	}

	public void setLimitStart(int start) {
		this.limit.setStart(start);
	}

	public void setLimitCount(int count) {
		this.limit.setCount(count);
	}

	public SelectSQL clone() {
		return ((SelectSQL) SerializeUtil.byteToObj(SerializeUtil.objToByte(this)));
	}

	public String getSQLString() {
		return getSQLString(ISQL.DBType.mysql);
	}

	public String getSQLString(ISQL.DBType type) {
		if (!(StringUtil.isNullOrEmpty(this.sql))) {
			return this.sql;
		}

		String field = (this.fields.size() == 0) ? "*" : this.fields.getSQLString(type);
		String sql = "";
		if ((type == ISQL.DBType.mysql) || (this.limit == null)
				|| ((this.limit.getCount() == 0) && (this.limit.getStart() == 0))) {
			sql = "select " + field + " from " + this.tables.getSQLString(type) + this.where.getSQLString(type)
					+ this.groupBy.getSQLString(type) + this.orderBy.getSQLString(type) + this.limit.getSQLString(type);
		} else {
			WhereSQL w1 = this.where.clone();
			w1.addCondition(new SQLCondition("rownum",
					String.valueOf(this.limit.getStart() + this.limit.getCount() + 1), "<", true));
			WhereSQL w2 = this.where.clone();
			w2.addCondition(new SQLCondition("rownum", String.valueOf(this.limit.getStart() + 1), "<", true));
			sql = "SELECT * FROM ( SELECT A.*, ROWNUM RNNNNNN FROM (SELECT " + field + " FROM "
					+ this.tables.getSQLString(type) + this.where.getSQLString(type) + this.groupBy.getSQLString(type)
					+ this.orderBy.getSQLString(type) + ") A WHERE ROWNUM <= "
					+ (this.limit.getStart() + this.limit.getCount()) + " ) WHERE RNNNNNN >= "
					+ (this.limit.getStart() + 1);
		}

		return SQLUtils.removeMultiBlank(sql.trim()).trim();
	}

	public String toString() {
		return getSQLString(ISQL.DBType.mysql);
	}
}