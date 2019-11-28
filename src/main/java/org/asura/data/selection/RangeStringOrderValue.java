package org.asura.data.selection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RangeStringOrderValue implements IOrderValue {
	private List<String> values;

	public RangeStringOrderValue() {
		this.values = new ArrayList();
	}

	public RangeStringOrderValue(String[] values) {
		this.values = new ArrayList();
		this.values.addAll(Arrays.asList(values));
	}

	public int getBlockIndex(String value) {
		for (int i = 0; i < this.values.size(); ++i) {
			if (((String) this.values.get(i)).equals(value)) {
				return 0;
			}
		}

		return -1;
	}

	public boolean contains(String value) {
		return this.values.contains(value);
	}

	public List<String> getValues() {
		return this.values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}
}