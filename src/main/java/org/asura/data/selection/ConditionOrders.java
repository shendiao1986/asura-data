package org.asura.data.selection;

import java.util.ArrayList;
import java.util.List;

public class ConditionOrders {
	private List<ConditionOrder> orders;

	public ConditionOrders() {
		this.orders = new ArrayList();
	}

	public ConditionOrders(IOrderValue orderValue) {
		this.orders = new ArrayList();
	}

	public List<ConditionOrder> getOrders() {
		return this.orders;
	}

	public void setOrders(List<ConditionOrder> orders) {
		this.orders = orders;
	}

	public DataBlocks sort(DataBlock block) {
		DataBlocks result = new DataBlocks();

		result.addDataBlock(block);
		DataBlocks temp = new DataBlocks();
		for (int i = 0; i < this.orders.size(); ++i) {
			temp.clear();

			result.clear();
			for (DataBlock db : temp.getBlocks()) {
				result.addDataBlock(db);
			}
		}

		return result;
	}
}
