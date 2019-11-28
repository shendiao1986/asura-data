package org.asura.data.selection;

public abstract class NotMeetFeatureCondition implements IFeatureCondition {
	private boolean notMeet;

	public NotMeetFeatureCondition() {
		this.notMeet = false;
	}

	public NotMeetFeatureCondition(boolean notMeet) {
		this.notMeet = notMeet;
	}

	public boolean isNotMeet() {
		return this.notMeet;
	}

	public void setNotMeet(boolean notMeet) {
		this.notMeet = notMeet;
	}
}
