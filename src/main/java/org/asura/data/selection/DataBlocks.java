package org.asura.data.selection;

import java.util.ArrayList;
import java.util.List;

public class DataBlocks {
	private List<DataBlock> blocks;

	public DataBlocks() {
		this.blocks = new ArrayList();
	}

	public void addDataBlock(DataBlock block) {
		this.blocks.add(block);
	}

	public void addDataAtBlock(IFeaturable data, int block) {
		while (this.blocks.size() <= block) {
			this.blocks.add(new DataBlock());
		}

		((DataBlock) this.blocks.get(block)).addData(data);
	}

	public void removeEmptyBlokcs() {
		List temp = new ArrayList();
		for (DataBlock db : this.blocks) {
			if (db.count() != 0) {
				temp.add(db);
			}
		}

		this.blocks = temp;
	}

	public DataBlocks andMerge(DataBlocks anotherBlocks) {
		DataBlocks result = new DataBlocks();
		for (DataBlock block : this.blocks) {
			for (DataBlock ab : anotherBlocks.getBlocks()) {
				result.addDataBlock(block.andDataBlock(ab));
			}
		}

		return result;
	}

	public List<IFeaturable> getAllDatas() {
		List list = new ArrayList();
		for (DataBlock bl : this.blocks) {
			list.addAll(bl.getDataList());
		}

		return list;
	}

	public DataBlocks appendMerge(DataBlocks anotherBlocks) {
		DataBlocks result = new DataBlocks();
		for (DataBlock block : this.blocks) {
			result.addDataBlock(block);
		}

		List list = result.getAllDatas();

		for (DataBlock block : anotherBlocks.getBlocks()) {
			block.removeDatas(list);
			result.addDataBlock(block);
		}

		return result;
	}

	public DataBlocks orMerge(DataBlocks anotherBlocks) {
		DataBlocks result = new DataBlocks();

		DataBlock db = new DataBlock();
		for (DataBlock block : this.blocks) {
			db = db.orDataBlock(block);
		}

		for (DataBlock ab : anotherBlocks.getBlocks()) {
			db = db.orDataBlock(ab);
		}

		result.addDataBlock(db);

		return result;
	}

	public void addDataBlocks(DataBlocks blocks) {
		this.blocks.addAll(blocks.getBlocks());
	}

	public List<DataBlock> getBlocks() {
		return this.blocks;
	}

	public void setBlocks(List<DataBlock> blocks) {
		this.blocks = blocks;
	}

	public void clear() {
		this.blocks.clear();
	}

	public String toString() {
		return super.getClass().getSimpleName() + "[" + this.blocks + "]";
	}
}
