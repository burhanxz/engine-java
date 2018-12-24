package com.alibabacloud.polar_race.engine.core;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.DynamicSliceOutput;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.bloomfilter.BloomFilter;
import com.alibabacloud.polar_race.engine.bloomfilter.BloomFilters;
import com.alibabacloud.polar_race.engine.bloomfilter.DynamicBloomFilter;
import com.google.common.base.Throwables;

public class TableBuilder {
	private static final ExecutorService genFilterPool = Executors.newFixedThreadPool(Util.BLOCK_NUMBER);
	/**
	 * 规定的Block尺寸
	 */
	// private final int blockSize;
	private final FileChannel fileChannel;
	private final BlockBuilder dataBlockBuilder;
	private final MetaBlockBuilder metaBlockBuilder;
	/**
	 * 记录上一次插入的key
	 */
	private Slice lastKey;
	private boolean closed;
	private long entryCount;

	private int tableOffset;

	// 文件指针位置
	private long position;

	public TableBuilder(FileChannel fileChannel) {
		requireNonNull(fileChannel, "fileChannel is null");
		try {
			checkState(position == fileChannel.position(), "Expected position %s to equal fileChannel.position %s",
					position, fileChannel.position());
		} catch (IOException e) {
			throw Throwables.propagate(e);
		}

		this.fileChannel = fileChannel;

		// blockSize = Util.blockSize;

		dataBlockBuilder = new BlockBuilder();
		metaBlockBuilder = new MetaBlockBuilder();

		lastKey = Slices.EMPTY_SLICE;
		tableOffset = 0;
	}

	public int currentTableSize() {
		return dataBlockBuilder.currentSizeEstimate();
	}

	public void add(Slice key, Slice value) throws IOException {
		assert key.length() == Util.SIZE_OF_KEY : "key数据长度错误";
		assert value.length() == Util.ADDR_SIZE : "addr数据长度错误";
		requireNonNull(value, "value is null");

		checkState(!closed, "table is finished");

		// 如果已经插入过数据，那么要保证当前插入的key > 之前最后一次插入的key，
		// SSTable必须是有序的插入数据
		// if (entryCount > 0) {
		// assert (userComparator.compare(key, lastKey) > 0) : "key must be greater than
		// last key";
		// }

		// If we just wrote a block, we can now add the handle to index block
		// 写索引块
		// if (pendingIndexEntry) {
		// checkState(dataBlockBuilder.isEmpty(),
		// "Internal error: Table has a pending index entry but data block builder is
		// empty");
		// // 找到前一个block和当前key之间的最短的字符串作为block分界Key，作为块索引
		// Slice shortestSeparator = Util.findShortestSeparator(lastKey, key);
		// // 对索引块进行信息编码
		// Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
		// // 将找到的shortest key 和encode后的块索引加入索引块中
		// indexBlockBuilder.add(shortestSeparator, handleEncoding);
		// pendingIndexEntry = false;
		// }
		// 如果使用了filter（leveldb中一般为bloomfilter
		/*
		 * if (r->filter_block != NULL) { r->filter_block->AddKey(key); }
		 */

		// 记录最后一次插入的key，插入数量，添加到数据块中
		lastKey = key;
		entryCount++;
		dataBlockBuilder.add(key, value);
		tableOffset += Util.SIZE_OF_BLOCK_ENTRY;
		// key写入metaBlock
		metaBlockBuilder.addKey(key);
		// 如果当前已插入的大小达到设定的block阈值，将block写到数据文件中
		int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
		if (estimatedBlockSize >= Util.blockSize) {
			assert estimatedBlockSize == Util.blockSize : "block数据长度错误";
			flush();
		}
	}

	private void flush() throws IOException {
		checkState(!closed, "table is finished");
		if (dataBlockBuilder.isEmpty()) {
			return;
		}
		writeBlock(dataBlockBuilder);

	}

	// 写入fileChannel
	private void writeBlock(BlockBuilder blockBuilder) throws IOException {
		// close the block
		// block组建完成

		Slice blockContents = blockBuilder.finish();
		// write data
		position += fileChannel.write(blockContents.toByteBuffer());
		// clean up state
		blockBuilder.reset();
	}

	public void finish() throws IOException, InterruptedException, ExecutionException {
		checkState(!closed, "table is finished");
		flush();
		int metaBlockOffset = tableOffset;
		closed = true;
		// 写meta block
		Slice metaBlock = metaBlockBuilder.finish();
		position += fileChannel.write(metaBlock.toByteBuffer());

		ByteBuffer offsetBuffer = ByteBuffer.wrap(Util.intToBytes(metaBlockOffset));
		position += fileChannel.write(offsetBuffer);

	}

	public static void close() {
		genFilterPool.shutdown();
		try {
			genFilterPool.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static class BlockBuilder {

		private int entryCount;
		/**
		 * 离上一个重启点有多少个record
		 */

		private boolean finished;
		private final DynamicSliceOutput block;
		private Slice lastKey;

		public BlockBuilder() {
			int estimatedSize = Util.blockSize;
			this.block = new DynamicSliceOutput(estimatedSize);
		}

		public void reset() {
			block.reset();
			entryCount = 0;
			lastKey = null;
			finished = false;
		}

		public void add(Slice key, Slice addr) {
			requireNonNull(key, "key is null");
			requireNonNull(addr, "value is null");
			checkState(!finished, "block is finished");
			// //一些基本的状态判断，如上次插入的key是否比当前key小等
			// checkArgument(lastKey == null || comparator.compare(key, lastKey) > 0, "key
			// must be greater than last key");

			// write key bytes 8B
			block.writeBytes(key);

			// write addr bytes 8B
			block.writeBytes(addr);

			// update last key
			lastKey = key;

			// update state
			entryCount++;
		}

		/**
		 * 重启点信息部分由Finish添加。 组建block data完成，返回block data
		 * 
		 * @return
		 */
		public Slice finish() {
			return block.slice();
		}

		public boolean isEmpty() {
			return entryCount == 0;
		}

		public int currentSizeEstimate() {
			return block.size(); // restart position size
		}
	}

	public static class MetaBlockBuilder {
		// DynamicBloomFilter bloomFilter = new DynamicBloomFilter();
		// private Slice results = Slices.allocate(Util.BLOCK_NUMBER *
		// Util.META_BLOCK_ENTRY_SIZE);
		private final Map<Integer, Future<Slice>> results = new HashMap<>();
		private SliceOutput sliceOutput;
		private int entries = 0; // 距上一次重置后写入key的数量
		private int metablockSize = 0;
		private boolean closed = false;
		private AtomicInteger blockNum = new AtomicInteger(0);

		public MetaBlockBuilder() {
			sliceOutput = new DynamicSliceOutput(Util.SIZE_OF_KEY * Util.BLOCK_ENTRY_NUMBER);
		}

		public void addKey(Slice key) {
			assert !closed : "此metablock builder已经关闭.";
			sliceOutput.writeBytes(key);
			entries++;
			// 加入了 num >= 1<<12的key，则截断，生成filter数据
			if (entries >= Util.BLOCK_ENTRY_NUMBER) {

				// 生成filter,耗时 50ms
				// genFilter();

				Slice keys = sliceOutput.slice();
				if (blockNum.get() == 0) {
					Slice keyy = keys.slice(0, 8);
					System.out.println(Util.bytesToLong(keyy.getBytes()));
				}
				Future<Slice> future = genFilterPool.submit(new genFilterTask(blockNum.get(), entries, keys));
				results.put(blockNum.getAndIncrement(), future);
				// 写入器复位
				sliceOutput = new DynamicSliceOutput(Util.SIZE_OF_KEY * Util.BLOCK_ENTRY_NUMBER);
				// 重置entry
				entries = 0;
			}

		}

		public Slice finish() throws InterruptedException, ExecutionException {
			// 生成最后一个filter result
			if (sliceOutput.size() > 0) {
				Slice keys = sliceOutput.slice();
				Future<Slice> future = genFilterPool.submit(new genFilterTask(blockNum.get(), entries, keys));
				results.put(blockNum.getAndIncrement(), future);
			}
			// 等待所有任务完成
			// genFilterPool.awaitTermination(1, TimeUnit.DAYS);
			// 分配空间
			// Slice ret = Slices.allocate(metablockSize);
			// 所有result附加到一起
			// SliceOutput output = ret.output();
			// System.out.println("results size: " + results.size());
			// for (int i = 0; i < blockNum.get(); i++) {
			// output.writeBytes(results.get(i));
			// }
			SliceOutput output = new DynamicSliceOutput(blockNum.get() * Util.META_BLOCK_ENTRY_SIZE);
			for (int i = 0; i < blockNum.get(); i++) {
				// 同步获取result
				Slice result = results.get(i).get();
				// 正确性测试
//				 Slice key = new Slice(Util.longToBytes(i * Util.BLOCK_ENTRY_NUMBER + 10l));
//				 assert BloomFilters.getBloomFilter().keyMayMatch(key, result) : "result不正确";
				output.writeBytes(result);
			}
			Slice ret = output.slice();
			// genFilterPool.shutdown();
			return ret;
		}

		private class genFilterTask implements Callable<Slice> {
			int blockNum;
			Slice keys;
			int keyCount;

			genFilterTask(int blockNum, int keyCount, Slice keys) {
				this.blockNum = blockNum;
				this.keyCount = keyCount;
				// 获取key
				this.keys = keys;
				// for(int i = 0; i < (1 << 12); i++) {
				// Slice key = keys.slice(i * 8, 8);
				// System.out.println(Util.bytesToLong(key.getBytes()));
				// }

			}

			@Override
			public Slice call() {
				Slice result = null;
				// BloomFilter bloomFilter = BloomFilters.getBloomFilter();
				DynamicBloomFilter bloomFilter = new DynamicBloomFilter();
				if (keys != null && keys.length() > 0) {

					// 根据获取的key生成filter,key数量为entries
					// long s = System.currentTimeMillis();
					Slice key = keys.slice(0, 8);

					result = bloomFilter.createFilter(keys, keyCount);

					assert BloomFilters.getBloomFilter().keyMayMatch(key, result) : "result不正确";

					// 更新Metablock总大小
					metablockSize += result.length();
					// long e = System.currentTimeMillis();
					// System.out.println("第" + blockNum + "组filter耗时： " + (e - s) + "ms.");

				}
				return result;
			}

		}
	}
}
