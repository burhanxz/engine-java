package com.alibabacloud.polar_race.engine.core;

import java.util.List;
import java.util.concurrent.Callable;

import com.alibabacloud.polar_race.engine.base.FileMetaData;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceComparator;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.core.Table.TableIterator;
import static com.alibabacloud.polar_race.engine.core.KeyCache.resultCache;

public class TableReadTask implements Callable<Slice> {
	private static final SliceComparator sliceComparator = new SliceComparator();

	private Slice key;

	public TableReadTask(Slice key) {
		this.key = key;
		System.out.println(Thread.currentThread().getName() + " readTask查询key: " + Util.bytesToLong(key.getBytes()));
	}

	@Override
	public Slice call() throws Exception {
		return find();
	}

	private Slice find() {
		if (resultCache.containsKey(key))
			return resultCache.get(key);

		List<FileMetaData> list = SSDManager.getInstance().getFiles();
		// table逐个查找
		for (FileMetaData fileMetaData : list) {
			// table逐个查找时，如果resultCache中已经存在key则直接获取并返回
			if (resultCache.containsKey(key))
				return resultCache.get(key);
			// 先看key是否在table的最小最大key范围内
			if (sliceComparator.compare(key, fileMetaData.getSmallest()) > 0
					&& sliceComparator.compare(key, fileMetaData.getLargest()) < 0) {

				// 获取table对象
				Table table = TableCache.getInstance().getTable(fileMetaData.getNumber());
				TableIterator it = table.iterator();
				// 获取位置信息(bloomFilter排除法)
				int location = table.getBlockLocation(key);
				// 查找每个block
				for (int j = 0; j < Util.BLOCK_NUMBER; j++) {
					// block逐个查找时，如果resultCache中已经存在key则直接获取并返回
					if (resultCache.containsKey(key))
						return resultCache.get(key);
					// 当某个block可能存在key的时候
					if ((location & (1 << j)) != 0) {
						it.setBlock(j);
						while (it.hasNext()) {
							Slice data = it.next();
							Slice key4Test = data.slice(0, Util.SIZE_OF_KEY);
							// 如果读取的test key大于key，则说明此block不可能存在key(比较排除法)
							if (sliceComparator.compare(key, key4Test) > 0) {
								break;
							}
							// 获取到key，结束循环
							if (sliceComparator.compare(key, key4Test) == 0) {
								Slice addr = data.slice(Util.SIZE_OF_KEY, Util.ADDR_SIZE);
								// 更新resultCache
								resultCache.putIfAbsent(key, addr);
								return addr;
							}
						}
						// 重置迭代器
						it.reset();

					}
				}

			}
		}

		return null;
	}

}
