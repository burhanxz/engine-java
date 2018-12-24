package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.preliminary.Works;

import com.carrotsearch.hppc.LongIntHashMap;
import com.sun.jna.Pointer;

import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public class FinalDIORegistry {
	private static final int PARALLELS = 4;
	private static final int THREADS = 64;
	private static final int CACHE_SIZE = 1 << 28;
	private static final int NODES_PER_CACHE = 1 << 16;

	private static DirectIoLib lib = DirectIoLib.getLibForPath(System.getProperty("java.io.tmpdir"));
	private static Field maskField;
	private static Field pointerField;
	static {
		try {
			maskField = LongIntHashMap.class.getDeclaredField("mask");
			maskField.setAccessible(true);
			pointerField = AlignedDirectByteBuffer.class.getDeclaredField("pointer");
			pointerField.setAccessible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private ThreadLocal<byte[]> bytes = new ThreadLocal<byte[]>() {
		@Override
		protected byte[] initialValue() {
			return new byte[Util.SIZE_OF_VALUE];
		}
	};

	private final File databaseDir;
	private Container[] containers = new Container[PARALLELS];
	private DirectIoByteChannel[] channels = new DirectIoByteChannel[Util.GROUPS];

	private AtomicInteger comingSeq;
	private CyclicBarrier resetBarrier;
	private AtomicBoolean hasReset;

	private AtomicBoolean isReady;
	private AtomicInteger toRegister;
	private AtomicBoolean shouldReset;

	private long s;
	private long s1;
	private volatile boolean isFirst = true;

	private byte[][] heapValues;
	public FinalDIORegistry(File databaseDir) throws Exception {
		this.databaseDir = databaseDir;
		// 初始化container
		for (int i = 0; i < PARALLELS; i++) {
			containers[i] = new Container();
		}
		// 初始化fileChannel
		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (logFile.exists()) {
				channels[i] = DirectIoByteChannel.getChannel(logFile, true);
			}
		}
		//初始化heapValues
		heapValues = new byte[NODES_PER_CACHE][Util.SIZE_OF_VALUE];
		
		// 状态参数初始化
		comingSeq = new AtomicInteger(0);
		resetBarrier = new CyclicBarrier(THREADS);
		hasReset = new AtomicBoolean(false);

		// reset参数初始化
		isReady = new AtomicBoolean(false);
		toRegister = new AtomicInteger(THREADS);
		shouldReset = new AtomicBoolean(false);
		s = System.currentTimeMillis();
		s1 = System.currentTimeMillis();
//		Works.getPool().execute(() -> {
//			try {
//				Thread.sleep(1000 * 60 * 4);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			System.exit(-1);
//			System.out.println("FinalRegistry 超时4min");
//		});
	}

	private void resetRange() {
		resetBarrier.reset();
		hasReset.set(false);

		isReady.set(false);
		toRegister.set(THREADS);
		isFirst = false;
		shouldReset.set(false);
		System.out.println("一轮range耗时 " + (System.currentTimeMillis() - s) + "ms.");
	}

	public void register(AbstractVisitor visitor) throws Exception {
		// 重置,需要保证所有线程同时执行range
		if (shouldReset.get()) {
			synchronized (this) {
				if (shouldReset.get()) {
					resetRange();
				}
			}
		}

		if (!isReady.get()) {
			synchronized (isReady) {
				if (!isReady.get()) {
					startCache();
					isReady.set(true);
				}
			}
		}

		readCache(visitor);

		// 每当register 64次之后，应重置
		if (toRegister.decrementAndGet() == 0) {
			shouldReset.set(true);
		}
	}

	public void readCache(AbstractVisitor visitor) throws Exception {

		int i = 0;
		for (int fileNum = Util.GROUPS - 1; fileNum >= 0; fileNum = nextGroup(fileNum)) {
			// 64个visit线程每次取的是同一个cotainer
			Container container = containers[i % PARALLELS];
			i++;
			container.full.acquire();

			// 取buffer副本
			AlignedDirectByteBuffer buffer = container.buffer;
			Pointer pointer = (Pointer) pointerField.get(buffer);
			
			//64线程合作将堆外value放入堆内
			for(;;) {
				int index = comingSeq.getAndIncrement();
				if(index >= NODES_PER_CACHE) {
					break;
				}
				pointer.read(index * Util.SIZE_OF_VALUE, heapValues[index], 0, Util.SIZE_OF_VALUE);
			}
			
			byte[] value = bytes.get();
			byte[] key;
			long keyVal;
			int addr;
			for (int x = 0; x < container.nodes; x++) {
				keyVal = container.keys[x];
				key = KeyEncoders.encode(keyVal);
				addr = container.map.get(keyVal);
				pointer.read(addr * 4096, value, 0, Util.SIZE_OF_VALUE);
//				visitor.visit(key, heapValues[addr]);
				visitor.visit(key, value);
			}

			// 等64个visit线程都到了
			hasReset.set(false);
			resetBarrier.await();
			if (!hasReset.get()) {
				synchronized (hasReset) {
					if (!hasReset.get()) {
						// 一次性释放所有
						container.empty.release(THREADS);
						System.out.println(fileNum + " visit完耗时 " + (System.currentTimeMillis() - s1) + "ms.");
						s1 = System.currentTimeMillis();
						comingSeq.set(0);
						resetBarrier.reset();
						hasReset.set(true);
					}
				}
			}
		}

	}

	private void startCache() {
		for (int i = 0; i < PARALLELS; i++) {
			final int i_ = i;
			Works.getPool().execute(() -> {
				for (int fileNum = nextGroup(Util.GROUPS - 1, i_); fileNum >= 0; fileNum = nextGroup(fileNum,
						PARALLELS)) {
					try {
						Container container = containers[i_];
						// 等待
						container.empty.acquire(THREADS);
						long s = System.currentTimeMillis();
						// 获取文件
						DirectIoByteChannel fileChannel = channels[fileNum];
						AlignedDirectByteBuffer buffer = container.buffer;
						// 缓存key和addr并排序
						int nodes = (int) (fileChannel.size() / Util.SIZE_OF_VALUE);
						container.nodes = nodes;
						container.fileNum = fileNum;
						if (!isFirst && nextGroup(fileNum, PARALLELS) < 0) {
							// 通知container结束key缓存线程
							container.end = true;
						}
						container.empty4k.release();

						buffer.clear();
						fileChannel.read(buffer, 0);

						// 等待key缓存完
						container.full4k.acquire();
						// 唤醒visit线程
						container.full.release(THREADS);
						System.out.println("文件 " + fileNum + " 读完耗时 " + (System.currentTimeMillis() - s) + "ms.");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
	}

	private void mkKeysAndAddrs(final int fileNum, final int nodes, final long[] keys, LongIntHashMap map)
			throws Exception {
		// 清空map
		map.clear();
		File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(fileNum));
		try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r"); FileChannel channel = raf.getChannel()) {
			MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * Util.SIZE_OF_KEY);

			long key;
			for (int x = 0; x < nodes; x++) {
				key = Util.getBigEndianLong(data);
				keys[x] = key;
				map.put(key, (x % NODES_PER_CACHE) * 4096);
			}

			Util.ByteBufferSupport.unmap(data);
		}
		// TODO 对keys排序
		// 获取map中的key
		long[] mapKeys = map.keys;
		// 获取map大小
		int mapSize = (int) maskField.get(map) + 1;
		int sortedKeyIndex = 0;
		long key;
		// 去重，去0
		for (int x = 0; x < mapSize && x < mapKeys.length; x++) {
			if ((key = mapKeys[x]) != 0) {
				keys[sortedKeyIndex++] = key;
			}
		}
		// key排序
		Arrays.sort(keys, 0, sortedKeyIndex);
	}

	private int nextGroup(int group, int dist) {
		int ret = group;
		for (int i = 0; i < dist; i++) {
			ret = nextGroup(ret);
		}
		return ret;
	}

	private int nextGroup(int group) {
		if (group == Util.GROUPS / 2) {
			return 0;
		} else if (group == (Util.GROUPS / 2) - 1) {
			return -1;
		} else if (group > Util.GROUPS / 2) {
			return --group;
		} else if (group < Util.GROUPS / 2 - 1 && group >= 0) {
			return ++group;
		} else { // group < 0
			return --group;
		}
	}

	public void close() throws Exception {
		for (int i = 0; i < Util.GROUPS; i++) {
			// if(channels[i] != null) {
			// channels[i].close();
			// }
		}
	}

	private class Container {
		int nodes;
		int fileNum;
		long[] keys;
		LongIntHashMap map;
		AlignedDirectByteBuffer buffer;
		AlignedDirectByteBuffer buffer4k;
		Semaphore full;
		Semaphore empty;
		Semaphore full4k;
		Semaphore empty4k;
		volatile boolean end;

		Container() {
			nodes = -1;
			fileNum = -1;
			keys = new long[NODES_PER_CACHE];
			map = new LongIntHashMap(NODES_PER_CACHE, 0.99);
			buffer = AlignedDirectByteBuffer.allocate(lib, CACHE_SIZE);
			buffer4k = AlignedDirectByteBuffer.allocate(lib, NODES_PER_CACHE * 8);
			full = new Semaphore(0);
			empty = new Semaphore(THREADS);
			full4k = new Semaphore(0);
			empty4k = new Semaphore(0);
			end = false;
			Works.getPool().execute(() -> {
				for (;;) {
					try {
						empty4k.acquire();

						mkKeysAndAddrs();
						// System.out.println("key " + fileNum + " over");
						full4k.release();
						if (end) {
							break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}

		void mkKeysAndAddrs() throws Exception {
			// 清空map
			map.clear();
			File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(fileNum));
			// try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r"); FileChannel
			// channel = raf.getChannel()) {
			// MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes *
			// Util.SIZE_OF_KEY);
			DirectIoByteChannel channel = DirectIoByteChannel.getChannel(kLogFile, true);
			buffer4k.clear();
			channel.read(buffer4k, 0);

			Pointer pointer = (Pointer) pointerField.get(buffer4k);

			byte[] key = new byte[8];
			long keyVal;
			for (int x = 0; x < nodes; x++) {
				pointer.read(x * 8, key, 0, 8);
				keyVal = Util.getBigEndianLong(key);
				keys[x] = keyVal;
				map.put(keyVal, x/* * 4096*/);
			}
			//
			// Util.ByteBufferSupport.unmap(data);
			// }
			// // 对keys排序
			// // 获取map中的key
			// long[] mapKeys = map.keys;
			// // 获取map大小
			// int mapSize = (int) maskField.get(map) + 1;
			// int sortedKeyIndex = 0;
			// long key;
			// // 去重，去0
			// for (int x = 0; x < mapSize && x < mapKeys.length; x++) {
			// if ((key = mapKeys[x]) != 0) {
			// keys[sortedKeyIndex++] = key;
			// }
			// }
			// key排序
			Arrays.sort(keys, 0, nodes);
		}
	}
}
