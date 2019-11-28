package org.asura.data.selection;

public class ConditionOrder {
	private IFeatureCondition condition;
	private ISelectOrder order;

	public ConditionOrder() {
	}

	public ISelectOrder getOrder() {
		return this.order;
	}

	public void setOrder(ISelectOrder order) {
		this.order = order;
	}

	public ConditionOrder(IFeatureCondition condition, ISelectOrder order) {
		this.condition = condition;
		this.order = order;
	}

	public ConditionOrder(IFeatureCondition condition, String feature, IOrderValue orderValue) {
		this.condition = condition;
		this.order = new FeatureSelectOrder(feature, orderValue);
	}

	public IFeatureCondition getCondition() {
		return this.condition;
	}

	public void setCondition(IFeatureCondition condition) {
		this.condition = condition;
	}
}
