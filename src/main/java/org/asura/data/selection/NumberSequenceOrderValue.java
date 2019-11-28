package org.asura.data.selection;

public class NumberSequenceOrderValue implements IOrderValue {
	private int max;
	private boolean desc;

	public NumberSequenceOrderValue() {
		this.max = 1000;
		this.desc = true;
	}

	public NumberSequenceOrderValue(int max) {
		this.max = max;
		this.desc = true;
	}

	public NumberSequenceOrderValue(int max, boolean desc) {
		this.max = max;
		this.desc = desc;
	}

	public int getBlockIndex(String value) {
		try {
			int iv = Integer.valueOf(value).intValue();
			if ((iv >= 0) && (iv <= this.max)) {
				if (!(this.desc)) {
					return iv;
				}
				return (this.max - iv);
			}
		} catch (Exception localException) {
		}
		return -1;
	}

	public boolean contains(String value) {
		try {
			int iv = Integer.valueOf(value).intValue();
			if ((iv >= 0) && (iv <= this.max))
				return true;
		} catch (Exception localException) {
		}
		return false;
	}

	public int getMax() {
		return this.max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public boolean isDesc() {
		return this.desc;
	}

	public void setDesc(boolean desc) {
		this.desc = desc;
	}

}
