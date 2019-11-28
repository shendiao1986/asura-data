package org.asura.data.sql;

public class SQLUtils {
	public static final String BLANK = " ";

	public static void main(String[] args) {
		System.out.println(getStandardSQLValue("sfww\\'dfw'"));
	}

	public static String getStandardSQLValue(String value) {
		if (value != null) {
			value = value.replace("\\", "\\\\");
			value = value.replace("'", "''");
		}

		return value;
	}

	public static String removeMultiBlank(String sql) {
		if (sql != null) {
			while (sql.contains("  ")) {
				sql = sql.replace("  ", " ");
			}
		}

		return sql;
	}
}
