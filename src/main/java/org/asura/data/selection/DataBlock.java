package org.asura.data.selection;

import java.util.ArrayList;
import java.util.List;

public class DataBlock {
	private List<IFeaturable> dataList;

	public DataBlock() {
		this.dataList = new ArrayList();
	}

	public void addData(IFeaturable data) {
		if (!(this.dataList.contains(data)))
			this.dataList.add(data);
	}

	public void addDatas(List<IFeaturable> list) {
		this.dataList.addAll(list);
	}

	public int count() {
		return this.dataList.size();
	}

	public List<IFeaturable> getDataList() {
		return this.dataList;
	}

	public void setDataList(List<IFeaturable> dataList) {
		this.dataList = dataList;
	}

	public void removeData(IFeaturable data) {
		this.dataList.remove(data);
	}

	public int size() {
		return this.dataList.size();
	}

	public void removeDatas(List<IFeaturable> datas) {
		for (IFeaturable data : datas)
			this.dataList.remove(data);
	}

	public DataBlock andDataBlock(DataBlock block) {
		DataBlock result = new DataBlock();
		for (IFeaturable data : this.dataList) {
			if (block.getDataList().contains(data)) {
				result.addData(data);
			}
		}

		return result;
	}

	public DataBlock orDataBlock(DataBlock block) {
		DataBlock result = new DataBlock();
		for (IFeaturable data : this.dataList) {
			result.addData(data);
		}

		for (IFeaturable data : block.getDataList()) {
			if (!(result.getDataList().contains(data))) {
				result.addData(data);
			}
		}

		return result;
	}

	public String toString() {
		return super.getClass().getSimpleName() + "[" + "dataList=" + this.dataList + "]";
	}
}
