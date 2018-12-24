package com.alibabacloud.polar_race.engine.bloomfilter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class ParallelBloomFilter implements BloomFilter{
	private final static int k = 6;
	private final static double bitPerKey = 8.656170245333781;

	private static final String hashName = "MD5"; // 在大多数情况下，MD5提供了较好的散列精确度。如有必要，可以换成 SHA1算法
	private static final BlockingQueue<MessageDigest> queue = new LinkedBlockingQueue<>(20);

	public ParallelBloomFilter() {
		IntStream.range(0, 20).parallel().forEach(index -> {
			MessageDigest tmp;
			try {
				tmp = java.security.MessageDigest.getInstance(hashName);
				queue.put(tmp);
			} catch (Exception e) {
				tmp = null;
			}
		});
	}

	public Slice createFilter(Slice keys, int keyCount) {
		int bitSetSize = (int) Math.ceil(bitPerKey * keyCount);

		BitSet bitset = new BitSet(bitSetSize);

		IntStream.range(0, keyCount).parallel().forEach(index -> {
			Slice key = keys.slice(index * Util.SIZE_OF_KEY, Util.SIZE_OF_KEY);
			add(key, bitset, bitSetSize);
		});
		Slice result = bitSetToResult(bitset, bitSetSize);
		// log.info("result.len: " + result.length());
		return result;
	}

	public boolean keyMayMatch(Slice key, Slice result) {
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
			bitset.set(Math.abs((int) hash), true);

		}
	}

	public long createHash(byte[] data) {
		long h = 0;
		byte[] res;
		MessageDigest digestFunction;
		try {
			digestFunction = queue.poll(1, TimeUnit.DAYS);
			res = digestFunction.digest(data);
			queue.put(digestFunction);

			for (int i = 0; i < 4; i++) {
				h <<= 8;
				h |= ((int) res[i]) & 0xFF;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return h;
	}

	public Slice bitSetToResult(BitSet bitSet, int bitSetSize) {
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
