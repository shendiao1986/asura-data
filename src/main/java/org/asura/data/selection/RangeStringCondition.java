package org.asura.data.selection;

import java.util.ArrayList;
import java.util.List;

public class RangeStringCondition implements IFeatureCondition {
	private List<String> strings;
	private boolean notContains;

	public RangeStringCondition() {
		this.strings = new ArrayList();
		this.notContains = false;
	}

	public RangeStringCondition(boolean notContains) {
		this.strings = new ArrayList();
		this.notContains = notContains;
	}

	public void addString(String value) {
		this.strings.add(value);
	}

	public boolean meet(String featureValue) {
		if (this.notContains) {
			return (!(this.strings.contains(featureValue)));
		}
		return this.strings.contains(featureValue);
	}

	public List<String> getStrings() {
		return this.strings;
	}

	public void setStrings(List<String> strings) {
		this.strings = strings;
	}

	public boolean isNotContains() {
		return this.notContains;
	}

	public void setNotContains(boolean notContains) {
		this.notContains = notContains;
	}
}
