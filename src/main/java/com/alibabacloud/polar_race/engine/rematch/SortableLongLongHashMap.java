package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.concurrent.Future;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.preliminary.ConcurrentLongLongHashMap;
import com.alibabacloud.polar_race.engine.preliminary.Works;
import com.carrotsearch.hppc.LongLongHashMap;

public class SortableLongLongHashMap extends ConcurrentLongLongHashMap {
	private final static int KEY_PAGE_SIZE = Util.KEY_PAGE;
	private FileChannel fileChannel;
	private MappedByteBuffer data;
	private long offset;

	public SortableLongLongHashMap(int expectedElements, double loadFactor) {
		super(expectedElements, loadFactor);
	}

	public int assigned() {
		return atomicAssigned.get();
	}

	public int positive() {
		return atomicPositive.get();
	}

	public int negtive() {
		return atomicAssigned.get() - atomicPositive.get();
	}

	public ArrayReader getSortedKeys(File databaseDir) throws Exception {
		// 新建有序数组
		System.out.println("assigned = " + atomicAssigned.get());
		System.out.println("positive = " + atomicPositive.get());
		System.out.println("negtive = " + (atomicAssigned.get() - atomicPositive.get()));
		System.out.println("hasEmptyKey = " + hasEmptyKey);
		int posSize = atomicPositive.get();
		int negSize = atomicAssigned.get() - atomicPositive.get();
		if (hasEmptyKey) {
			posSize++;
		}
		long[] posData = new long[posSize];
		long[] negData = new long[negSize];
		int size = posData.length + negData.length;
		int posIndex = 0;
		int negIndex = 0;
		// 复制keys到data
		if (hasEmptyKey) {
			posData[posIndex++] = 0l;
		}
		// 遍历keys
		for (int i = 0; i < mask + 1; i++) {
			long key = keys[i];
			if (key > 0) {
				posData[posIndex++] = key;
			} else if (key < 0) {
				negData[negIndex++] = -key;
			}
		}
		System.out.println("posIndex = " + posIndex);
		System.out.println("negIndex = " + negIndex);
		// TODO 可并行
//		Future<?> future1 = Works.getPool().submit(()->{
//			radixSort(posData);
//		});
//		Future<?> future2 = Works.getPool().submit(()->{
//			radixSort(negData);
//		});
		
//		future1.get();
//		future2.get();
		radixSort(posData);
		radixSort(negData);
//		return new MemoryReader(negData, posData);
		
		long s = System.currentTimeMillis();

		// 将数组持久化到磁盘
		File sortedKeyFile = new File(databaseDir, "sortedKey");
		RandomAccessFile raf = new RandomAccessFile(sortedKeyFile, "rw");
		this.fileChannel = raf.getChannel();
		this.data = fileChannel.map(MapMode.READ_WRITE, 0, KEY_PAGE_SIZE);
		this.offset = 0;
		for (int index = 0; index < size; index++) {
			ensureRoom();
			long keyVal = index >= negSize ? posData[index - negSize] : -negData[negSize - 1 - index];
			byte[] key = KeyEncoders.encode(keyVal);
			// System.out.println("index = " + index + " keyVal = " + keyVal);
			data.put(key);
		}
		Util.ByteBufferSupport.unmap(data);
		fileChannel.close();
		raf.close();
		System.out.println("持久化key耗时: " + (System.currentTimeMillis() - s) + "ms.");
		return new ArrayReader(size, databaseDir, this);
	}

	private void ensureRoom() throws Exception {
		if (data.position() == KEY_PAGE_SIZE) {
			Util.ByteBufferSupport.unmap(data);
			offset += Util.KEY_PAGE;
			data = fileChannel.map(MapMode.READ_WRITE, offset, KEY_PAGE_SIZE);
		}
	}

	public static class MemoryReader {
		final int size;
		final int negSize;
		final long[] negData;
		final long[] posData;

		public MemoryReader(long[] negData, long[] posData) {
			this.negData = negData;
			this.posData = posData;
			this.size = negData.length + posData.length;
			this.negSize = negData.length;
		}

		public long get(int index) {
			if (index >= negData.length) {
				return posData[index - negSize];
			} else {
				return -negData[negSize - 1 - index];
			}
		}
		
		public int size() {
			return size;
		}
	}

	public static class ArrayReader {
		final int size;
		private FileChannel fileChannel;
		private LongLongHashMap map;

		public ArrayReader(int size, File databaseDir, LongLongHashMap map) throws Exception {
			this.size = size;
			this.map = map;
			// 将数组持久化到磁盘
			File sortedKeyFile = new File(databaseDir, "sortedKey");
			RandomAccessFile raf = new RandomAccessFile(sortedKeyFile, "r");
			this.fileChannel = raf.getChannel();
		}

		public void getKeys(long[] keyVals, int index) throws Exception {
			int length = 8 * keyVals.length;
			if ((index + keyVals.length) > size) {
				length = (size - index) * 8;
			}
			long pos = index * 8;
			MappedByteBuffer data = fileChannel.map(MapMode.READ_ONLY, pos, length);
			byte[] temp = new byte[8];
			int x = 0;
			while (data.hasRemaining()) {
				data.get(temp);
				keyVals[x++] = Util.getBigEndianLong(temp);
			}
			Util.ByteBufferSupport.unmap(data);
		}

		public void getKeys(byte[][] keys, int[] fileNums, long[] fileOffsets, int index) throws Exception {
			int length = 8 * fileNums.length;
			if ((index + fileNums.length) > size) {
				length = (size - index) * 8;
			}
			long pos = index * 8;
			MappedByteBuffer data = fileChannel.map(MapMode.READ_ONLY, pos, length);
			long keyVal = 0l;
			long addrVal = 0l;
			int x = 0;
			while (data.hasRemaining()) {
				data.get(keys[x]);
				keyVal = Util.getBigEndianLong(keys[x]);
				addrVal = map.get(keyVal);
				fileNums[x] = (int) (addrVal >>> 48);
				fileOffsets[x] = addrVal & Util.ADDR_MASK;
				x++;
			}
			Util.ByteBufferSupport.unmap(data);
		}

		public int size() {
			return size;
		}

		public long get(int index) {
			throw new UnsupportedOperationException();
		}

		// public Iterator<Long> iterator(){
		// return new ArrayIterator();
		// }
		//
		// public Iterator<Long> iterator(long lowerVal){
		// return new ArrayIterator(lowerVal);
		// }
		//
		// private class ArrayIterator implements Iterator<Long>{
		// private int index;
		// ArrayIterator(){
		// index = 0;
		// }
		// ArrayIterator(long lowerVal){
		// for(index = 0; index < size; index++) {
		// if(index >= negData.length) {
		// if(posData[index - negSize] >= lowerVal)
		// break;
		// }
		// else {
		// if(-negData[negSize - 1 - index] >= lowerVal)
		// break;
		// }
		// }
		// }
		// @Override
		// public boolean hasNext() {
		// return index < size;
		// }
		// @Override
		// public Long next() {
		// if(index >= negData.length) {
		// return posData[(index++) - negSize];
		// }
		// else {
		// return -negData[negSize - 1 -(index++)];
		// }
		// }
		//
		// }
	}

	private void radixSort(long[] data) {
		int radix = 1 << 16;
		int d = 4;
		// 缓存数组
		long[] tmp = new long[data.length];

		int[] buckets = new int[radix];

		for (int i = 0; i < d; i++) {
			Arrays.fill(buckets, 0);

			for (int j = 0; j < data.length; j++) {
				int subKey = ((int) (data[j] >>> (i * 16))) & 0xffff;
				buckets[subKey]++;
			}

			for (int j = 1; j < radix; j++) {
				buckets[j] = buckets[j] + buckets[j - 1];
			}

			// swap
			long[] c = tmp;
			tmp = data;
			data = c;
			// 最耗时
			final int b = (i * 16);
			int subKey = 0;
			final int mask = 0xffff;
			for (int m = data.length - 1; m >= 0; m--) {
				subKey = ((int) (tmp[m] >>> b)) & mask;
				data[--buckets[subKey]] = tmp[m];
			}
		}

	}

}
