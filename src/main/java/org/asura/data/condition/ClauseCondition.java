package org.asura.data.condition;

import java.util.List;

public class ClauseCondition<T> implements ICondition<T> {
	private List<ICondition<T>> ands;
	private List<ICondition<T>> ors;
	private List<ICondition<T>> nots;

	public void addAndClause(ICondition<T> condition) {
		this.ands.add(condition);
	}

	public void addNotClause(ICondition<T> condition) {
		this.nots.add(condition);
	}

	public void addOrClause(ICondition<T> condition) {
		this.ors.add(condition);
	}

	public boolean meet(T t) {
		if (this.ands != null) {
			for (ICondition<T> con : this.ands) {
				if (!(con.meet(t))) {
					return false;
				}
			}
		}

		if (this.nots != null) {
			for (ICondition<T> con : this.nots) {
				if (con.meet(t)) {
					return false;
				}
			}
		}

		if ((this.ors != null) && (this.ors.size() > 0)) {
			boolean meet = false;
			for (ICondition<T> con : this.ors) {
				if (con.meet(t)) {
					meet = true;
					break;
				}
			}

			if (!(meet)) {
				return false;
			}

		}

		return true;
	}

	public ICondition<T> parse(String expression) {
		return null;
	}

}
