package org.asura.data.selection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BooleanFeatureCondition implements IFeatureCondition {
	private List<IFeatureCondition> orList;
	private List<IFeatureCondition> andList;

	public BooleanFeatureCondition() {
		this.orList = new ArrayList();
		this.andList = new ArrayList();
	}

	public void addOrSelectOrder(IFeatureCondition order) {
		this.orList.add(order);
	}

	public void addAndSelectOrder(IFeatureCondition order) {
		this.andList.add(order);
	}

	public boolean meet(String featureValue) {
		for (IFeatureCondition order : this.andList) {
			if (!(order.meet(featureValue))) {
				return false;
			}
		}

		if (this.orList.size() > 0) {
			boolean meet = false;
			for (Iterator iterator1 = orList.iterator(); iterator1.hasNext();) {
				IFeatureCondition order = (IFeatureCondition) iterator1.next();
				if (order.meet(featureValue)) {
					meet = true;
					break;
				}
			}

			return meet;
		} else {
			return true;
		}
	}
}
