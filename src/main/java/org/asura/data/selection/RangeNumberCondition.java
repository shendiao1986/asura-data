package org.asura.data.selection;

public class RangeNumberCondition extends NotMeetFeatureCondition {
	private double start;
	private double end;
	private boolean containsStart;
	private boolean containsEnd;

	public RangeNumberCondition() {
	}

	public RangeNumberCondition(double start, double end) {
		this.start = start;
		this.end = end;
	}

	public RangeNumberCondition(double start, double end, boolean containsStart, boolean containsEnd) {
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

	public boolean meet(String featureValue) {
		try {
			double d = Double.valueOf(featureValue).doubleValue();
			if (isNotMeet()) {
				return (!(meet(d)));
			}
			return meet(d);
		} catch (Exception localException) {
		}
		return false;
	}

	private boolean meet(double d) {
		if ((this.containsStart) && (this.containsEnd))
			return ((d >= this.start) && (d <= this.end));
		if (this.containsStart)
			return ((d >= this.start) && (d < this.end));
		if (this.containsEnd) {
			return ((d > this.start) && (d < this.end));
		}
		return ((d > this.start) && (d < this.end));
	}
}
