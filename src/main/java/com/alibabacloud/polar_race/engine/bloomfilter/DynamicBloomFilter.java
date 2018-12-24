package com.alibabacloud.polar_race.engine.bloomfilter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Random;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

//TODO fork-join框架使用
public class DynamicBloomFilter implements BloomFilter {
	// private final static double falsePositiveProbability =
	// Util.falsePositiveProbability;
	/*
	 * 0.03 k = 6 bitPerKey = 8.656170245333781
	 */
	/*
	 * 0.01 7 10.098865286222745
	 */
	/*
	 * 0.001 10 14.426950408889635
	 */
	/*
	 * 0.005 8 11.541560327111707
	 */
	private final static int k = Util.k;
	private final static double bitPerKey = Util.bitPerKey;

	private static final String hashName = "MD5"; // 在大多数情况下，MD5提供了较好的散列精确度。如有必要，可以换成 SHA1算法
	private final MessageDigest digestFunction;// MessageDigest类用于为应用程序提供信息摘要算法的功能，如 MD5 或 SHA 算法
	// static {
	// k = (int) Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))); //
	// k = ln(2)m/n
	// bitPerKey = Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) /
	// Math.log(2);// c = k/ln(2)
	// }

	public DynamicBloomFilter() {
		// 初始化 MessageDigest 的摘要算法对象
		MessageDigest tmp;
		try {
			tmp = java.security.MessageDigest.getInstance(hashName);
		} catch (NoSuchAlgorithmException e) {
			tmp = null;
		}
		digestFunction = tmp;
	}

	public Slice createFilter2(Slice keys, int keyCount) {
		int bitSetSize = (int) Math.ceil(bitPerKey * keyCount);

		BitSet bitset = new BitSet(bitSetSize);
		System.out.println("(1)bitSet.size() = " + bitset.size());
		System.out.println("(1)bitSetSize = " + bitSetSize);
		for (int i = 0; i < keys.length(); i += Util.SIZE_OF_KEY) {
			add(keys.slice(i, Util.SIZE_OF_KEY), bitset, bitSetSize);
		}
		Slice result = bitSetToResult(bitset, bitSetSize);
		// log.info("result.len: " + result.length());
		return result;
	}

	// result = bitset的byte[]形式 + bitsetSize
	public boolean keyMayMatch(Slice key, Slice result) {
		int offset = result.length() - Util.SIZE_OF_INT;
		int bitSetSize4Result = result.getInt(offset);
		Slice resultSlice = result.slice(0, offset);
		// BitSet bitset4Result = Util.sliceToBitSet(resultSlice);
		long hash;
		byte[] data = new byte[Util.SIZE_OF_KEY + Util.SIZE_OF_INT];
		for (int i = 0; i != Util.SIZE_OF_KEY; i++) {
			data[i] = key.getByte(i);
		}

		for (int x = 0; x < k; x++) {
			data[Util.SIZE_OF_KEY + 3] = (byte) ((x >> 24) & 0xFF);
			data[Util.SIZE_OF_KEY + 2] = (byte) ((x >> 16) & 0xFF);
			data[Util.SIZE_OF_KEY + 1] = (byte) ((x >> 8) & 0xFF);
			data[Util.SIZE_OF_KEY] = (byte) (x & 0xFF);
			hash = createHash(data);
			hash = hash % (long) bitSetSize4Result;
			// 优化省去bitset
			int index = Math.abs((int) hash);
			int i = index / 8;
			int j = 7 - index % 8;
			boolean ret = (resultSlice.getByte(i) & (1 << j)) >> j == 1 ? true : false;
			if (!ret)
				return false;
			// assert bitset4Result.get(Math.abs((int) hash)) == ret : "结果不一致";
			// if (!bitset4Result.get(Math.abs((int) hash)))
			// return false;
		}
		return true;
	}

	public Slice createFilter(Slice keys, int keyCount) {
		int bitSetSize = (int) Math.ceil(bitPerKey * keyCount);
		Slice result = Slices.allocate((makeSize(bitSetSize) / 8) + Util.SIZE_OF_INT);
		for (int i = 0; i < keys.length(); i += Util.SIZE_OF_KEY) {
			add2Slice(keys.slice(i, Util.SIZE_OF_KEY), result, bitSetSize);
		}
		result.setInt(result.length() - Util.SIZE_OF_INT, bitSetSize);
		// log.info("result.len: " + result.length());
		return result;
	}
	
	public void add2Slice(Slice key, Slice result, int bitSetSize) {
		long hash;
		byte[] data = new byte[Util.SIZE_OF_KEY + Util.SIZE_OF_INT];
		for (int i = 0; i != Util.SIZE_OF_KEY; i++) {
			data[i] = key.getByte(i);
		}
		for (int x = 0; x < k; x++) {
			data[Util.SIZE_OF_KEY + 3] = (byte) ((x >> 24) & 0xFF);
			data[Util.SIZE_OF_KEY + 2] = (byte) ((x >> 16) & 0xFF);
			data[Util.SIZE_OF_KEY + 1] = (byte) ((x >> 8) & 0xFF);
			data[Util.SIZE_OF_KEY] = (byte) (x & 0xFF);
			hash = createHash(data);
			hash = hash % (long) bitSetSize;
			int i = Math.abs((int) hash);
			//slice代替bitset
			int index = i / 8;
			int offset = 7 - i % 8;
			byte b = result.getByte(index);
			b |= 1 << offset;
			result.setByte(index, b);

			// bitset.set(Math.abs((int) hash), true);

		}
	}

	public void add(Slice key, BitSet bitset, int bitSetSize) {
		long hash;
		byte[] data = new byte[Util.SIZE_OF_KEY + Util.SIZE_OF_INT];
		for (int i = 0; i != Util.SIZE_OF_KEY; i++) {
			data[i] = key.getByte(i);
		}

		for (int x = 0; x < k; x++) {
			data[Util.SIZE_OF_KEY + 3] = (byte) ((x >> 24) & 0xFF);
			data[Util.SIZE_OF_KEY + 2] = (byte) ((x >> 16) & 0xFF);
			data[Util.SIZE_OF_KEY + 1] = (byte) ((x >> 8) & 0xFF);
			data[Util.SIZE_OF_KEY] = (byte) (x & 0xFF);
			hash = createHash(data);
			hash = hash % (long) bitSetSize;
			assert Math.abs((int) hash) < bitset.size() : "hash越界";
			bitset.set(Math.abs((int) hash), true);

		}
	}

	public long createHash(byte[] data) {
		long h = 0;
		byte[] res;

		synchronized (digestFunction) {
			res = digestFunction.digest(data);
		}

		for (int i = 0; i < 4; i++) {
			h <<= 8;
			h |= ((int) res[i]) & 0xFF;
		}

		return h;
	}

	public Slice bitSetToResult(BitSet bitSet, int bitSetSize) {
		if (bitSet.size() != bitSetSize) {
			System.out.println("bitSet.size() = " + bitSet.size());
			System.out.println("bitSetSize = " + bitSetSize);
		}
		Slice slice = Slices.allocate((bitSet.size() / 8) + Util.SIZE_OF_INT);
		for (int i = 0; i < bitSet.size(); i++) {
			int index = i / 8;
			int offset = 7 - i % 8;
			byte b = slice.getByte(index);
			b |= (bitSet.get(i) ? 1 : 0) << offset;
			slice.setByte(index, b);
		}
		slice.setInt(slice.length() - Util.SIZE_OF_INT, bitSetSize);
		return slice;
	}

	private int makeSize(int initSize) {
		return (((initSize - 1) >> 6) + 1) * 64;
	}
	
	// 测试
	public static void main(String[] args) {
		DynamicBloomFilter bloomFilter = new DynamicBloomFilter();
		DynamicBloomFilter bloomFilter2 = new DynamicBloomFilter();

		final int entrySize = 1 << 12;
		Random random = new Random();
		Slice keys = Slices.allocate(entrySize * Util.SIZE_OF_KEY);
		SliceOutput sliceOutput = keys.output();
		byte[] testData = new byte[Util.SIZE_OF_KEY];
		for (int i = 0; i != entrySize; i++) {
			random.nextBytes(testData);
			sliceOutput.writeBytes(testData);
		}
		Slice result = bloomFilter.createFilter(keys, entrySize);
		System.out.println("result len: " + result.length());
		// System.out.println(bf.contains(element));

		for (int i = 0; i != entrySize * Util.SIZE_OF_KEY; i += Util.SIZE_OF_KEY) {
			Slice key = keys.slice(i, Util.SIZE_OF_KEY);
			// System.out.println("(正确数据)是否匹配： " + bloomFilter2.keyMayMatch(key, result));
			assert bloomFilter2.keyMayMatch(key, result) : "正确数据不匹配";
		}
		// for (int i = 0; i != entrySize * Util.SIZE_OF_KEY; i += Util.SIZE_OF_KEY) {
		// random.nextBytes(testData);
		// Slice key = new Slice(testData);
		// System.out.println("(错误数据)是否匹配： " + bloomFilter2.keyMayMatch(key, result));
		// }
	}
}
