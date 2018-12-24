package com.alibabacloud.polar_race.engine.preliminary;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import com.carrotsearch.hppc.LongLongHashMap;

@SuppressWarnings("restriction")
public class CASLongLongHashMap extends LongLongHashMap {
	private static sun.misc.Unsafe unsafe;
	// TODO 在正确的基础上考虑删除用原生类型
	// private AtomicInteger atomicAssign = new AtomicInteger(0);
	static {
		try {
			// 通过反射获取rt.jar下的Unsafe类
			Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			unsafe = (sun.misc.Unsafe) field.get(null);
		} catch (Exception e) {
			System.out.println("Get Unsafe instance occur error" + e);
		}
	}

	public CASLongLongHashMap(int expectedElements, double loadFactor) {
		super(expectedElements, loadFactor);
//		Array.newInstance(byte.class, 10);
//		unsafe.set
//		unsafe.arrayBaseOffset(byte[].class);
////		unsafe.getByte(arg0, arg1)
	}
	


	// 仅仅适用于没有remove操作的hashmap
	public long put(long key, long value, long x) {
		assert assigned < mask + 1;

		// final int mask = this.mask;
		if (((key) == 0)) {
			hasEmptyKey = true;
			long previousValue = values[mask + 1];
			values[mask + 1] = value;
			return previousValue;
		} else {
			int slot = hashKey(key) & mask;
			for (final long[] keys = this.keys, values = this.values;;) {
				// 真正引起并发错误的地方
				if (keys[slot] == 0) {
					if (assigned == resizeAt) {
						// 这里应该不会发生
						synchronized (this) {
							allocateThenInsertThenRehash(slot, key, value);
						}
					} else {
						// 理论上，如果这句cas成功，以后再也不会有key cas成功了
						boolean isOK = unsafe.compareAndSwapLong(keys, slot, 0l, key);
						if (isOK) {
							values[slot] = value;
							assigned++;
							return 0L;
						}
					}
				}

				else {
					if (keys[slot] == key) {
						final long previousValue = values[slot];
						values[slot] = value;
						return previousValue;
					}
				}
				slot = (slot + 1) & mask;
			}
		}

	}

	// @Override
	public long put(long key, long value, boolean b) {
		assert assigned < mask + 1;

		final int mask = this.mask;
		if (((key) == 0)) {
			hasEmptyKey = true;
			long previousValue = values[mask + 1];
			values[mask + 1] = value;
			return previousValue;
		} else {
			final long[] keys = this.keys;
			int slot = hashKey(key) & mask;

			// long existing;
			for (;;) {
				while (keys[slot] != 0l) {
					if (keys[slot] == key) {
						synchronized ((Object) slot) {
							if (keys[slot] == key) {
								final long previousValue = values[slot];
								values[slot] = value;
								return previousValue;
							}
						}
					}
					slot = (slot + 1) & mask;
				}

				if (assigned == resizeAt) {
					synchronized (this) {
						if (assigned == resizeAt) {
							System.out.println("真的扩容了???");
							allocateThenInsertThenRehash(slot, key, value);
							return 0L;
						}
					}

				} else {
					boolean isOK = unsafe.compareAndSwapLong(keys, slot, 0l, key);
					if (isOK) {
						// System.out.println("cas>>>>");
						values[slot] = value;
						assigned++;
						return 0L;
					}
					slot = (slot + 1) & mask;
				}

				// assigned++;
				// return 0L;
			}

		}
	}

}
