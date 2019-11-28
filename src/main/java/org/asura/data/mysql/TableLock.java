package org.asura.data.mysql;

import java.util.ArrayList;
import java.util.List;

import org.asura.core.data.DataIterator;
import org.asura.core.data.DataRecord;
import org.asura.core.util.StringUtil;
import org.asura.data.sql.SelectSQL;

public class TableLock {
	public static final String TableStatus = "table_status";
	private static MysqlHandler handler = new MysqlHandler();

	public static String getLastestVersion(String category, String table) {
		SelectSQL sql = new SelectSQL();
		sql.setSql("select * from table_status where name = '" + table + "' and category = '" + category
				+ "' order by version desc limit 1");

		String version = null;

		DataIterator it = handler.select(new SelectSQL());
		if (it.hasNext()) {
			version = ((DataRecord) it.next()).getFieldValue("version");
		}
		it.close();

		return version;
	}

	public static String[] getAllVersion(String category, String table) {
		SelectSQL sql = new SelectSQL();
		sql.setSql("select * from table_status where name = '" + table + "' and category = '" + category
				+ "' order by version desc");

		List versions = new ArrayList();

		DataIterator it = handler.select(sql);
		while (it.hasNext()) {
			versions.add(((DataRecord) it.next()).getFieldValue("version"));
		}
		it.close();

		return ((String[]) versions.toArray(new String[0]));
	}

	private static void setStatus(String category, String table, String version, Status sta) {
		handler.execute("update table_status set status = '" + sta.name().toLowerCase() + "' where name = '" + table
				+ "' and version = '" + version + "' and category = '" + category + "'");
	}

	private static void addStatus(String category, String table, String version, Status sta) {
		handler.execute("replace into table_status set status = '" + sta.name().toLowerCase() + "', name = '" + table
				+ "', version = '" + version + "', category = '" + category + "'");
	}

	public static void startRead(String category, String table, String version) {
		setStatus(category, table, version, Status.Reading);
	}

	public static void endRead(String category, String table, String version) {
		setStatus(category, table, version, Status.Both);
	}

	public static void check(String category, String table, String version) {
		setStatus(category, table, version, Status.Checked);
	}

	public static void startWrite(String category, String table, String version) {
		addStatus(category, table, version, Status.Writing);
	}

	public static void endWrite(String category, String table, String version) {
		setStatus(category, table, version, Status.Writen);
	}

	public static String getStatus(String category, String table, String version) {
		SelectSQL sql = new SelectSQL("table_status");
		sql.addWhereCondition("name", table);
		sql.addWhereCondition("category", category);
		sql.addWhereCondition("version", version);

		String sta = "";
		DataIterator it = handler.select(sql);
		if (it.hasNext()) {
			sta = ((DataRecord) it.next()).getFieldValue("status");
		}
		it.close();

		return sta;
	}

	public static boolean isWritting(String category, String table, String version) {
		String sta = getStatus(category, table, version);
		return Status.Writing.name().equalsIgnoreCase(sta);
	}

	public static boolean isReading(String category, String table, String version) {
		String sta = getStatus(category, table, version);
		return Status.Reading.name().equalsIgnoreCase(sta);
	}

	public static boolean isUnknow(String category, String table, String version) {
		String sta = getStatus(category, table, version);
		return StringUtil.isNullOrEmpty(sta);
	}

	public static boolean canRead(String category, String table, String version) {
		String sta = getStatus(category, table, version);
		return ((!(Status.Writing.name().equalsIgnoreCase(sta))) && (!(Status.Writen.name().equalsIgnoreCase(sta)))
				&& (!(StringUtil.isNullOrEmpty(sta))));
	}

	public static boolean hasRead(String category, String table, String version) {
		String sta = getStatus(category, table, version);
		return Status.Both.name().equalsIgnoreCase(sta);
	}

	public static boolean canReadOnlyOnce(String category, String table, String version) {
		String sta = getStatus(category, table, version);
		return Status.Checked.name().equalsIgnoreCase(sta);
	}

	public static enum Status {
		Reading, Writing, Writen, Both, Checked;
	}
}
