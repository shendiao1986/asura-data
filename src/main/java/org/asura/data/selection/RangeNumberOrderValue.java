package org.asura.data.selection;

public class RangeNumberOrderValue implements IOrderValue {
	private double start;
	private double end;
	private boolean containsStart;
	private boolean containsEnd;

	public RangeNumberOrderValue() {
	}

	public RangeNumberOrderValue(double start, double end) {
		this.start = start;
		this.end = end;
	}

	public RangeNumberOrderValue(double start, double end, boolean containsStart, boolean containsEnd) {
		this.start = start;
		this.end = end;
		this.containsStart = containsStart;
		this.containsEnd = containsEnd;
	}

	public double getStart() {
		return this.start;
	}

	public void setStart(double start) {
		this.start = start;
	}

	public double getEnd() {
		return this.end;
	}

	public void setEnd(double end) {
		this.end = end;
	}

	public boolean isContainsStart() {
		return this.containsStart;
	}

	public void setContainsStart(boolean containsStart) {
		this.containsStart = containsStart;
	}

	public boolean isContainsEnd() {
		return this.containsEnd;
	}

	public void setContainsEnd(boolean containsEnd) {
		this.containsEnd = containsEnd;
	}

	public boolean contains(String value) {
		try {
			Double d = Double.valueOf(value);
			if ((d.doubleValue() > this.start) && (d.doubleValue() < this.end))
				return true;
			if ((this.containsStart) && (d.doubleValue() == this.start)) {
				return true;
			}
			return ((this.containsEnd) && (d.doubleValue() == this.end));
		} catch (Exception localException) {
		}

		return false;
	}

	public int getBlockIndex(String value) {
		try {
			Double d = Double.valueOf(value);
			if ((d.doubleValue() > this.start) && (d.doubleValue() < this.end))
				return 0;
			if ((this.containsStart) && (d.doubleValue() == this.start))
				return 0;
			if ((this.containsEnd) && (d.doubleValue() == this.end)) {
				return 0;
			}
			return -1;
		} catch (Exception localException) {
		}
		return -1;
	}
}
