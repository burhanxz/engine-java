package com.alibabacloud.polar_race.engine.preliminary;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.hppc.LongLongHashMap;

@SuppressWarnings("restriction")
public class ConcurrentLongLongHashMap extends LongLongHashMap {
	protected final static int SEGMENTS = 1 << 8;
	protected final static int BASE_SEGMENT = (1 << 26) / SEGMENTS;
	protected final Object[] objects = new Object[SEGMENTS];
	protected AtomicInteger atomicAssigned = new AtomicInteger(0);
	protected AtomicInteger atomicPositive = new AtomicInteger(0);
	protected static sun.misc.Unsafe unsafe;
	static {
		try {
			Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			unsafe = (sun.misc.Unsafe) field.get(null);
		} catch (Exception e) {
			System.out.println("Get Unsafe instance occur error" + e);
		}
	}

	public ConcurrentLongLongHashMap(int expectedElements, double loadFactor) {
		super(expectedElements, loadFactor);
		for (int i = 0; i < SEGMENTS; i++) {
			objects[i] = new Object();
		}
	}

	private Object segmentFor(int slot) {
		int segment = slot / BASE_SEGMENT;
		if (segment >= SEGMENTS) {
			segment = SEGMENTS - 1;
		}
		return objects[segment];
	}

	public long syncPut(final long key, final long value) {
		assert atomicAssigned.get() < mask + 1;

		final int mask = this.mask;
		// TODO 隐患
		if (((key) == 0)) {
			synchronized (this) {
				hasEmptyKey = true;
				long previousValue = values[mask + 1];
				values[mask + 1] = value;
				return previousValue;
			}
		} else {
			final long[] keys = this.keys;
			int slot = hashKey(key) & mask;

			Object lock = segmentFor(slot);

			synchronized (lock) {
				long existing;
				while (!((existing = keys[slot]) == 0)) {
					if (((existing) == (key))) {
						final long previousValue = values[slot];
						values[slot] = value;
						return previousValue;
					}
					slot = (slot + 1) & mask;
				}

				if (atomicAssigned.get() == resizeAt) {
					System.out.println("扩容了！！！");
					allocateThenInsertThenRehash(slot, key, value);
				} else {
					keys[slot] = key;
					values[slot] = value;
				}

				atomicAssigned.incrementAndGet();
				if (key > 0)
					atomicPositive.incrementAndGet();
				return 0L;
			}

		}
	}

	public long syncPut2(final long key, final long value) {
		assert atomicAssigned.get() < mask + 1;

		final int mask = this.mask;
		// TODO 隐患
		if (((key) == 0)) {
			synchronized (this) {
				hasEmptyKey = true;
				long previousValue = values[mask + 1];
				values[mask + 1] = value;
				return previousValue;
			}
		} else {
			final long[] keys = this.keys;
			int slot = hashKey(key) & mask;

			Object lock = segmentFor(slot);

			synchronized (lock) {
				long existing;
				for (;;) {
					existing = keys[slot];
					// !=0
					if (existing != 0) {
						if (((existing) == (key))) {
							final long previousValue = values[slot];
							values[slot] = value;
							return previousValue;
						}
					}
					// ==0
					else {
						// cas判断 慢
						if (unsafe.compareAndSwapLong(keys, sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET + slot * 8, 0l,
								key)) {
							values[slot] = value;
							atomicAssigned.incrementAndGet();
							if (key > 0)
								atomicPositive.incrementAndGet();
							return 0L;
						}
					}
					slot = (slot + 1) & mask;
				}

			}

		}
	}

	public long get(long key, int slot, long existing) {
		if (((key) == 0)) {
			return hasEmptyKey ? values[mask + 1] : 0L;
		} else {
			slot = hashKey(key) & mask;
			while (!((existing = keys[slot]) == 0)) {
				if (((existing) == (key))) {
					return values[slot];
				}
				slot = (slot + 1) & mask;
			}

			return 0L;
		}
	}
}
