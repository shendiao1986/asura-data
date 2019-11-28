package org.asura.data.selection;

public class FeatureSelectOrder implements ISelectOrder {
	private String feature;
	private IOrderValue value;

	public FeatureSelectOrder() {
	}

	public FeatureSelectOrder(String feature, IOrderValue value) {
		this.feature = feature;
		this.value = value;
	}

	public String getFeature() {
		return this.feature;
	}

	public void setFeature(String feature) {
		this.feature = feature;
	}

	public IOrderValue getValue() {
		return this.value;
	}

	public void setValue(IOrderValue value) {
		this.value = value;
	}

	public DataBlocks sort(DataBlock block) {
		DataBlocks result = new DataBlocks();
		if (this.value instanceof CharOrderValue) {
			((CharOrderValue) this.value).reset();
		}
		for (IFeaturable data : block.getDataList()) {
			String fv = data.getFeatureValue(this.feature);
			if (this.value.contains(fv)) {
				result.addDataAtBlock(data, this.value.getBlockIndex(fv));
			}
		}

		return result;
	}
}
