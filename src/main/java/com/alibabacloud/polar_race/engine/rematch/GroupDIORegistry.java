package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.carrotsearch.hppc.LongLongHashMap;

import net.smacke.jaydio.DirectRandomAccessFile;

public class GroupDIORegistry {
	private static final int MAX_CACHES = 7;
	private static final int THREADS = 64;
	private static final int CACHE_SIZE = 1 << 27;
//	private static final int CACHE_PER_THREADS = CACHE_SIZE / THREADS;
	private static final int BUFFER_NODES = CACHE_SIZE / Util.SIZE_OF_VALUE; // 128M / 4K
	private static final int MAX_KEYS = MAX_CACHES * BUFFER_NODES;
	private static Field maskField;
	
	static {
		try {
			maskField = LongLongHashMap.class.getDeclaredField("mask");
			maskField.setAccessible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private final File databaseDir;
	
	private final ThreadLocal<byte[]> bytes = new ThreadLocal<byte[]>() {
		@Override
		protected byte[] initialValue() {
			return new byte[Util.SIZE_OF_VALUE];
		}
	};
	
	private long time;
	private final DirectRandomAccessFile[] drafs = new DirectRandomAccessFile[Util.GROUPS]; 
	private final int endFileNum;
	private final byte[][] caches = new byte[MAX_CACHES][];
	private final AtomicBoolean[] permits = new AtomicBoolean[MAX_CACHES];

	private AtomicBoolean isReady;

	private AtomicInteger toRegister;
	private AtomicBoolean shouldReset;

	private volatile int lowerIndex;
	private volatile int upperIndex;
	private Semaphore empty1;
	private Semaphore full1;
	private Semaphore empty2;
	private Semaphore full2;
	private volatile int remaining;
//	private AtomicInteger toReach;
	private AtomicBoolean toEnd;
	
	private Container container1;
	private Container container2;

	private AtomicBoolean isFirst;
	
	public GroupDIORegistry(File databaseDir) throws Exception {
		this.databaseDir = databaseDir;
		
		int end = Util.GROUPS - 1;
		for (int i = Util.GROUPS - 1; i != -1; i = nextGroup(i)) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (logFile.exists()) {
				drafs[i] = new DirectRandomAccessFile(logFile, "r");
				end = i;
			}
		}
		endFileNum = end;
		
		//初始化缓存和缓存许可
		for (int cacheIndex = 0; cacheIndex < MAX_CACHES; cacheIndex++) {
			caches[cacheIndex] = new byte[CACHE_SIZE];
			permits[cacheIndex] = new AtomicBoolean(true);
		}
		
		isReady = new AtomicBoolean();
		toRegister = new AtomicInteger();
		shouldReset = new AtomicBoolean();
		
		isReady.set(false);
		toRegister.set(THREADS);
		shouldReset.set(false);
		
		lowerIndex = 0;
		upperIndex = 0;
		
		empty1 = new Semaphore(THREADS);
		full1 = new Semaphore(0);
		empty2 = new Semaphore(THREADS);
		full2 = new Semaphore(0);
		remaining = MAX_CACHES;
		
//		toReach = new AtomicInteger();
		toEnd = new AtomicBoolean();
		
//		toReach.set(THREADS);
		toEnd.set(false);
		
		container1 = new Container(1);
		container2 = new Container(2);
		time = System.currentTimeMillis();
		isFirst = new AtomicBoolean(true);
	}

	private void resetRange() {
		//初始化缓存许可
		for (int cacheIndex = 0; cacheIndex < MAX_CACHES; cacheIndex++) {
			permits[cacheIndex].set(true);
		}
		isReady.set(false);
		toRegister.set(THREADS);
		
		
		lowerIndex = 0;
		upperIndex = 0;
		
		empty1 = new Semaphore(THREADS);
		full1 = new Semaphore(0);
		empty2 = new Semaphore(THREADS);
		full2 = new Semaphore(0);
		remaining = MAX_CACHES;
//		toReach.set(THREADS);
		toEnd.set(false);
		isFirst.set(false);
		shouldReset.set(false);
		System.out.println("reset range");
	}

	public void register() throws Exception {
		// 重置,需要保证所有线程同时执行range
		if (shouldReset.get()) {
			synchronized (this) {
				if (shouldReset.get()) {
					resetRange();
				}
			}
		}

		// 开启写线程
		if (!isReady.get()) {
			synchronized (this) {
				if (!isReady.get()) {
					startWriteTask();
					isReady.set(true);
				}
			}
		}

		System.out.println(Thread.currentThread().getName() + " register");
		// 每当register 64次之后，应重置
		if (toRegister.decrementAndGet() == 0) {
			shouldReset.set(true);
		}
	}

	public void readCache(AbstractVisitor visitor) throws Exception {
		// 设置最高优先级
//		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
		boolean flag4Read = true;
		Semaphore currentFull;
		Semaphore currentEmpty;
		Container container;
		for (;;) {
			if (flag4Read) {
				currentFull = full1;
				currentEmpty = empty1;
				container = container1;
			} else {
				currentFull = full2;
				currentEmpty = empty2;
				container = container2;
			}
			flag4Read = !flag4Read;
//			System.out.println(Thread.currentThread().getName() + " : " + container.toString());
			try {
				currentFull.acquire();
				// TODO 实际读取 有隐患
				int lower = this.lowerIndex;
				int upper = this.upperIndex;
				long s = System.currentTimeMillis();
				for (int i = lower; i < upper; i++) {
					//获取地址
					long keyVal = container.sortedKeys[i - lower];
					long addrVal = container.map.get(keyVal);
					int bufferNum = (int) (addrVal >>> 48);
					int bufferOffset = (int) (addrVal & Util.ADDR_MASK);
					//读buffer缓存数据
					byte[] key = KeyEncoders.encode(keyVal);
					byte[] value = getValue(caches[bufferNum], bufferOffset);
					visitor.visit(key, value);
				}
				System.out.println(Thread.currentThread().getName() + ", " + lower + " ~ " + upper + " 耗时： " + (System.currentTimeMillis() - s) + "ms.");
				currentEmpty.release();
				if (container.toReach.decrementAndGet() == 0) {
					System.out.println("释放 " + container.toString());
					int buffers = container.buffers;
					container.release();
					remaining += buffers;
					
					System.out.println("lower = " + lower + ", upper = " + upper + ". 归还container");
					container.resetToReach();
				}
				if (toEnd.get()) {
					System.out.println("结束位置： " + upper);
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void startWriteTask() throws Exception {
		new Thread(() -> {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

			try {
				Semaphore currentFull;
				Semaphore currentEmpty;
				Container container;
				boolean flag = true;
				for (int i = Util.GROUPS - 1; i != -1; i = nextGroup(i)) {
					/* 获取容器和信号量 */
					if (flag) {
						currentFull = full1;
						currentEmpty = empty1;
						container = container1;
					} else {
						currentFull = full2;
						currentEmpty = empty2;
						container = container2;
					}
					flag = !flag;
//					System.out.println("获取 " + container.toString());
					
					/* 获取文件 */
					DirectRandomAccessFile draf = null;
					// 跳过空文件
					if ((draf = drafs[i]) == null) {
						flag = !flag;
						continue;
					}
					// 获取文件大小
					long fileSize = draf.length();
					int buffers = (int) Math.ceil(fileSize * 1.0 / CACHE_SIZE);

					/* container */
					if (remaining < buffers) {
						System.out.println(i + " 等待container");
						for (;;) {
							if (remaining >= buffers) {
								System.out.println(i + " 得到container");
								break;
							}
						}
					}
					remaining -= buffers;
					// 容器申请缓存
					container.applyFor(buffers);

					/* 开始 */
					currentEmpty.acquire(THREADS);
					long s = System.currentTimeMillis();
					// 实际操作
					// 缓存key值
					final int nodes = (int) (draf.length() / Util.SIZE_OF_VALUE);
					System.out.println("fileNum = " + i + ", nodes = " + nodes);
					File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i));
					try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r");
							FileChannel channel = raf.getChannel()) {
						MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);

						int bufferIndex = 0;
						int[] bufferNums = container.bufferNums;
//						System.out.println("bufferNums = ");
//						for(int x = 0; x < container.buffers; x++) {
//							System.out.print(bufferNums[x] + ", ");
//						}
//						System.out.println();
						int bufferNum = 0;
						// nodes是包含重复key的
						for (int x = 0; x < nodes; x++) {
							if (x % BUFFER_NODES == 0) {
								bufferNum = bufferNums[bufferIndex++];
//								System.out.println("x = " + x + ", bufferNum = " + bufferNum);
							}
							// 获取key
							long keyVal = Util.getBigEndianLong(data);
							long fileNum = ((long) bufferNum << 48) | Util.ADDR_INIT;
							long offset = (x % BUFFER_NODES) * 4096l;
							long addrVal = offset | fileNum;
							container.map.put(keyVal, addrVal);
						}
						Util.ByteBufferSupport.unmap(data);
					}
					// 对key进行排序
					long[] keys = container.map.keys;
					int sortedKeyIndex = 0;
					long key;
					int mapSize = (int) maskField.get(container.map) + 1;
//					System.out.println("mapSize = " + mapSize);
					for (int keyIndex = 0; keyIndex < mapSize; keyIndex++) {
						if ((key = keys[keyIndex]) != 0) {
							container.sortedKeys[sortedKeyIndex++] = key;
						}
					}
//					System.out.println("nodes = " + nodes + ", sortedKeyIndex = " + sortedKeyIndex);
					Arrays.sort(container.sortedKeys, 0, sortedKeyIndex);

					// 缓存value
					for (int x = 0; x < container.buffers; x++) {
						int cacheIndex = container.bufferNums[x];
//						System.out.println("缓存第 " + i + " 个buffer");
						byte[] buffer = caches[cacheIndex];
						draf.read(buffer);
					}

					// 更新range位置
					this.lowerIndex = this.upperIndex;
					this.upperIndex += sortedKeyIndex;
					System.out.println(i + " 写完了");
					
					// 设置结束
					if (i == endFileNum) {
						toEnd.set(true);
						System.out.println("本次range耗时: " + (System.currentTimeMillis() - time) + "ms.");
						time = System.currentTimeMillis();
					}
					System.out.println("缓存 " + lowerIndex + " ~ " + upperIndex + " 耗时： " + (System.currentTimeMillis() - s) + "ms.");
					currentFull.release(64);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}

	private int nextGroup(int group) {
		if (group == Util.GROUPS / 2) {
			return 0;
		} else if (group == (Util.GROUPS / 2) - 1) {
			return -1;
		} else if (group > Util.GROUPS / 2) {
			return --group;
		}
		// group < Util.GROUPS / 2 - 1
		else {
			return ++group;
		}
	}
	
	private byte[] getValue(byte[] buffer, int index) {
		byte[] value = bytes.get();
		System.arraycopy(buffer, index, value, 0, Util.SIZE_OF_VALUE);
		return value;
	}

	public void close() throws Exception {
		for (int i = 0; i < Util.GROUPS; i++) {
			if (drafs[i] != null) {
				drafs[i].close();
			}
		}
	}

	private class Container {
		final int num;
		AtomicBoolean permit = new AtomicBoolean(true);
		LongLongHashMap map = new LongLongHashMap(MAX_KEYS, 0.99);
		long[] sortedKeys = new long[MAX_KEYS];
		int[] bufferNums = new int[MAX_CACHES];
		int buffers = 0;
		AtomicInteger toReach = new AtomicInteger(THREADS);
		
		public Container(int num) {
			this.num = num;
		}

		void applyFor(int buffers) {
			// 调用之前已经确保remaining足够
			if(!permit.get()) {
				System.out.println("applyFor 申请container");
				for(;;) {
					if(permit.get()) {
						System.out.println("applyFor 获得container");
						break;
					}
				}
			}
			// 关闭容器许可
			permit.set(false);
			// map清空
			map.clear();
			this.buffers = buffers;
			for (int cacheIndex = 0, x = 0; x < buffers && cacheIndex < MAX_CACHES; cacheIndex++) {
				// 缓存可以使用
				if (permits[cacheIndex].get()) {
					bufferNums[x++] = cacheIndex;
					permits[cacheIndex].set(false);
				}
			}

		}

		void release() {

			for (int j = 0; j < buffers; j++) {
				int cacheIndex = bufferNums[j];
				permits[cacheIndex].set(true);
			}
			// buffers清0
			buffers = 0;
			// 打开许可
			permit.set(true);
		}
		
		void resetToReach() {
			toReach.set(THREADS);
		}

		@Override
		public String toString() {
			return "container " + num;
		}
	}
}
