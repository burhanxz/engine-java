package com.alibabacloud.polar_race.engine.bloomfilter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class BloomFilterUtil {
	private final static double falsePositiveProbability = Util.falsePositiveProbability;
	private final static int k;
	private final static double bitPerKey;

	private static final String hashName = "MD5"; // 在大多数情况下，MD5提供了较好的散列精确度。如有必要，可以换成 SHA1算法
	private static final MessageDigest digestFunction;// MessageDigest类用于为应用程序提供信息摘要算法的功能，如 MD5 或 SHA 算法
	static {
		k = (int) Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))); // k = ln(2)m/n
		bitPerKey = Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2);// c = k/ln(2)
		// 初始化 MessageDigest 的摘要算法对象
		MessageDigest tmp;
		try {
			tmp = java.security.MessageDigest.getInstance(hashName);
		} catch (NoSuchAlgorithmException e) {
			tmp = null;
		}
		digestFunction = tmp;
	}

	public static Slice createFilter(Slice keys, int keyCount) {
		int bitSetSize = (int) Math.ceil(bitPerKey * keyCount);

		BitSet bitset = new BitSet(bitSetSize);

		for (int i = 0; i < keys.length(); i += Util.SIZE_OF_KEY) {
			add(keys.slice(i, Util.SIZE_OF_KEY), bitset, bitSetSize);
		}
		Slice result = bitSetToResult(bitset, bitSetSize);
//		log.info("result.len: " + result.length());
		return result;
	}

	// result = bitset的byte[]形式 + bitsetSize
	public static boolean keyMayMatch(Slice key, Slice result) {
		int offset = result.length() - Util.SIZE_OF_INT;
		int bitSetSize4Result = result.getInt(offset);
		BitSet bitset4Result = Util.sliceToBitSet(result.slice(0, offset));
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
			if (!bitset4Result.get(Math.abs((int) hash)))
				return false;
		}
		return true;
	}

	public static void add(Slice key, BitSet bitset, int bitSetSize) {
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
			bitset.set(Math.abs((int) hash), true);
		}

	}

	public static long createHash(byte[] data) {
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

	public static Slice bitSetToResult(BitSet bitSet, int bitSetSize) {
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

	// 测试
	public static void main(String[] args) {
		final int entrySize = 1 << 12;
		Random random = new Random();
		Slice keys = Slices.allocate(entrySize * Util.SIZE_OF_KEY);
		SliceOutput sliceOutput = keys.output();
		byte[] testData = new byte[Util.SIZE_OF_KEY];
		for (int i = 0; i != entrySize; i++) {
			random.nextBytes(testData);
			sliceOutput.writeBytes(testData);
		}
		Slice result = BloomFilterUtil.createFilter(keys, entrySize);
		System.out.println("result len: " + result.length());
		// System.out.println(bf.contains(element));

		for (int i = 0; i != entrySize * Util.SIZE_OF_KEY; i += Util.SIZE_OF_KEY) {
			Slice key = keys.slice(i, Util.SIZE_OF_KEY);
			System.out.println("(正确数据)是否匹配： " + BloomFilterUtil.keyMayMatch(key, result));
		}
		for (int i = 0; i != entrySize * Util.SIZE_OF_KEY; i += Util.SIZE_OF_KEY) {
			random.nextBytes(testData);
			Slice key = new Slice(testData);
			System.out.println("(错误数据)是否匹配： " + BloomFilterUtil.keyMayMatch(key, result));
		}
	}
}
