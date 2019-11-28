package org.asura.data;

import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.asura.core.data.DataRecord;

public class DataObjectRecord extends DataRecord {
	private static final long serialVersionUID = 5651741206764994854L;
	private HashMap<String, Object> dataMap;
	private DataRecord.DataAction dataAction;

	public DataObjectRecord() {
		this.dataMap = new HashMap<>();
		this.dataAction = DataRecord.DataAction.Add;
	}

	public String[] getAllFields() {
		return ((String[]) this.dataMap.keySet().toArray(new String[0]));
	}

	public String getFieldValue(String field) {
		if (!(this.dataMap.containsKey(field))) {
			return "";
		}
		return this.dataMap.get(field).toString();
	}

	public Object getFieldObject(String field) {
		return this.dataMap.get(field);
	}

	public void deleteField(String field) {
		this.dataMap.remove(field);
	}

	public boolean containsField(String field) {
		return this.dataMap.containsKey(field);
	}

	public void setAction(DataRecord.DataAction action) {
		this.dataAction = action;
	}

	public void AddField(String field, String value) {
		this.dataMap.put(field, value);
	}

	public void AddField(String field, Object value) {
		this.dataMap.put(field, value);
	}

	public DataRecord.DataAction getDataAction() {
		String value = getFieldValue("action");
		if (value != null)
			try {
				value = StringUtils.capitalize(value);
				setAction(DataRecord.DataAction.valueOf(value));
			} catch (Exception localException) {
			}
		return this.dataAction;
	}

	public String toString() {
		return this.dataMap.toString();
	}

	@SuppressWarnings("unchecked")
	public DataRecord clone() {
		DataObjectRecord dr = new DataObjectRecord();
		dr.dataMap = (HashMap<String, Object>) this.dataMap.clone();
		dr.dataAction = this.dataAction;

		return dr;
	}
}
