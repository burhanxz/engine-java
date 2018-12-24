package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.preliminary.ConcurrentLongLongHashMap;
import com.alibabacloud.polar_race.engine.preliminary.Works;
import com.carrotsearch.hppc.LongIntHashMap;

public class GroupDB {
	private final static int NODES_PER_GROUP = 1 << 16;
	private final File databaseDir;
	private volatile GroupLogs logs;
	
	private volatile GroupLogReader reader;
	private volatile GroupBinaryLogReader binaryReader;
	
	private volatile ConcurrentLongLongHashMap map;
	private volatile LongIntHashMap[] maps;
	
	private volatile CountDownLatch latch;
	
	private volatile GroupRegistry registry1;
//	private volatile FinalDIORegistry registry2;
	private volatile FinalDIORegistry registry2;
	
	private volatile boolean isWellDistributed;
	private AtomicBoolean selected = new AtomicBoolean(false);
	private AtomicBoolean recovered = new AtomicBoolean(false);
	private byte[][] keys;
	private int[] addrs;
	private int[] allNodes;
	private int[] starts;
	private AtomicInteger reads = new AtomicInteger(0);
	public GroupDB(String path) throws Exception {
		System.out.println("open: " + path);
		databaseDir = new File(path);
		if (!databaseDir.exists()) {
			databaseDir.mkdirs();
			System.out.println("初始化db目录 = " + path);
		}
		// FileManager.init(databaseDir);
	}

	public void write(byte[] key, byte[] value) throws Exception {
		// 创建logs
		if (logs == null) {
			synchronized (this) {
				if (logs == null) {
					logs = new GroupLogs(databaseDir);
				}
			}
		}
		logs.add(key, value);
	}

	public byte[] read(byte[] key) throws Exception {
			// 建立map和reader
			if (!recovered.get()) {
				synchronized (recovered) {
					if (!recovered.get()) {
//						map = new ConcurrentLongLongHashMap(Util.MAP_SIZE, 0.99);
//						long s = System.currentTimeMillis();
//						parallelRecover();
//						System.out.println("recover耗时： " + (System.currentTimeMillis() - s) + "ms.");
//						reader = new GroupLogReader(databaseDir);
						maps = new LongIntHashMap[Util.GROUPS];
						long s = System.currentTimeMillis();
						groupRecover();
						System.out.println("recover耗时： " + (System.currentTimeMillis() - s) + "ms.");
						binaryReader = new GroupBinaryLogReader(databaseDir);
						recovered.set(true);
					}
				}
			}
			long keyVal = Util.getBigEndianLong(key);
			// 获取key高10位
			int fileNum = ((key[0] & 0xff) << 2) | ((key[1] & 0xff) >> 6);
			int addrVal = maps[fileNum].get(keyVal);
			if (addrVal == 0) {
				return null;
			}
			addrVal--; // 防0处理后还原
			byte[] value = binaryReader.getValue(fileNum, addrVal);
			return value;
	}

	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws Exception {
		if (!selected.get()) {
			synchronized (selected) {
				if (!selected.get()) {
					File logFile = new File(databaseDir, Util.Filename.logFileName(Util.GROUPS - 1));
					if (!logFile.exists()) {
						isWellDistributed = false;
					} else {
						isWellDistributed = true;
					}
					selected.set(true);
				}
			}
		}
		// 分布不均匀
		if (!isWellDistributed) {
			// 第一个线程初始化registry
			if (registry1 == null) {
				synchronized (this) {
					if (registry1 == null) {
						registry1 = new GroupRegistry(databaseDir);
					}
				}
			}
			// 第一个线程初始化latch
			if (latch == null) {
				synchronized (this) {
					if (latch == null) {
						latch = new CountDownLatch(64);
					}
				}
			}
			// 确保range同时开始
			latch.countDown();
			latch.await();
			// 确保所有线程都通过了await()
			Thread.sleep(100);
			if (latch != null) {
				synchronized (this) {
					if (latch != null) {
						latch = null;
					}
				}
			}

			registry1.register();
			registry1.readCache(visitor);
		}
		// 分布均匀
		else {
			// 第一个线程初始化registry
			if (registry2 == null) {
				synchronized (this) {
					if (registry2 == null) {
						registry2 = new FinalDIORegistry(databaseDir);
					}
				}
			}
			// 第一个线程初始化latch
			if (latch == null) {
				synchronized (this) {
					if (latch == null) {
						latch = new CountDownLatch(64);
					}
				}
			}
			// 确保range同时开始
			latch.countDown();
			latch.await();
			// 确保所有线程都通过了await()
			Thread.sleep(100);
			if (latch != null) {
				synchronized (this) {
					if (latch != null) {
						latch = null;
					}
				}
			}

			registry2.register(visitor);
		}

	}
	
	private void groupRecover() throws Exception{
		int sum = 0;
		final CountDownLatch waitForRecover = new CountDownLatch(Util.GROUPS);
		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				// 文件不存在，直接countDown
				waitForRecover.countDown();
				continue;
			}
			RandomAccessFile raf4v = new RandomAccessFile(logFile, "r");
			long fileSize = raf4v.length();
			raf4v.close();
			final int nodes = (int) (fileSize / Util.SIZE_OF_VALUE);
			System.out.println("i = " + i + ", nodes = " + nodes);
			sum += nodes;

			final int i_ = i;
			Works.getPool().execute(() -> {
				File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i_));
				try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r");
						FileChannel channel = raf.getChannel()) {
					MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);

					maps[i_] = new LongIntHashMap(nodes, 0.99);
					LongIntHashMap thisMap = maps[i_];
					for (int x = 0; x < nodes; x++) {
						// 获取key
						long keyVal = Util.getBigEndianLong(data);
						int addrVal = x * 4096 + 1; // 防0处理

						thisMap.put(keyVal, addrVal);
					}
					Util.ByteBufferSupport.unmap(data);
					// 文件存在，等recover完进行countDown
					waitForRecover.countDown();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});

		}
		waitForRecover.await();
		System.out.println("sum = " + sum);
	}

	private void parallelRecover() throws Exception {
		int sum = 0;
		final CountDownLatch waitForRecover = new CountDownLatch(Util.GROUPS);
		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				// 文件不存在，直接countDown
				waitForRecover.countDown();
				continue;
			}
			RandomAccessFile raf4v = new RandomAccessFile(logFile, "r");
			long fileSize = raf4v.length();
			raf4v.close();
			final int nodes = (int) (fileSize / Util.SIZE_OF_VALUE);
			System.out.println("i = " + i + ", nodes = " + nodes);
			sum += nodes;
			final long fileNum = (long) i << 48;
			final long initAddr = Util.ADDR_INIT | fileNum;
			final int i_ = i;
			Works.getPool().execute(() -> {
				File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i_));
				try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r");
						FileChannel channel = raf.getChannel()) {
					MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
					for (int x = 0; x < nodes; x++) {
						// 获取key
						long keyVal = Util.getBigEndianLong(data);

						long offset = x * 4096l;
						long addrVal = offset | initAddr;
						map.syncPut2(keyVal, addrVal);
					}
					Util.ByteBufferSupport.unmap(data);
					// 文件存在，等recover完进行countDown
					waitForRecover.countDown();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});

		}
		waitForRecover.await();
		System.out.println("sum = " + sum);
	}
	
	private void recover() throws Exception {
		int sum = 0;
		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				continue;
			}
			RandomAccessFile raf4v = new RandomAccessFile(logFile, "r");
			long fileSize = raf4v.length();
			raf4v.close();
			final int nodes = (int) (fileSize / Util.SIZE_OF_VALUE);
			System.out.println("i = " + i + ", nodes = " + nodes);
			sum += nodes;
			final long fileNum = (long) i << 48;
			final long initAddr = Util.ADDR_INIT | fileNum;
			File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i));
			try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r"); FileChannel channel = raf.getChannel()) {
				MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
				for (int x = 0; x < nodes; x++) {
					// 获取key
					long keyVal = Util.getBigEndianLong(data);

					long offset = x * 4096l;
					long addrVal = offset | initAddr;
					map.put(keyVal, addrVal);
				}
				Util.ByteBufferSupport.unmap(data);
			}
		}
		System.out.println("sum = " + sum);
	}

	private void slicedRecover() throws Exception {
		int sum = 0;
		final int nodesPerLog = Util.LOG_FILE_SIZE / Util.SIZE_OF_VALUE;
		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				continue;
			}
			final int nodes = Util.slicedRealNodes(databaseDir, i);
			System.out.println("i = " + i + ", nodes = " + nodes);
			sum += nodes;
			int fileNum = i;
			long initAddr = Util.ADDR_INIT | ((long) fileNum << 48);
			File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i));
			try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r"); FileChannel channel = raf.getChannel()) {
				MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
				for (int x = 0; x < nodes; x++) {
					if (x != 0 && x % nodesPerLog == 0) {
						fileNum += Util.GROUPS;
						initAddr = Util.ADDR_INIT | ((long) fileNum << 48);
					}
					// 获取key
					long keyVal = Util.getBigEndianLong(data);

					long offset = (x % nodesPerLog) * 4096l;
					long addrVal = offset | initAddr;
					map.put(keyVal, addrVal);
				}
				Util.ByteBufferSupport.unmap(data);
			}
		}
		System.out.println("sum = " + sum);
	}

	public synchronized void close() throws Exception {
		if (logs != null) {
			logs.close();
		}
		if (reader != null) {
			reader.close();
		}
		if (binaryReader != null) {
			binaryReader.close();
		}
		if (registry1 != null) {
			registry1.close();
		}

		if (registry2 != null) {
			registry2.close();
		}

		Works.close();

		System.out.println("关闭：" + databaseDir.getAbsolutePath());
		for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
			long count = gc.getCollectionCount();
			long time = gc.getCollectionTime();
			String name = gc.getName();
			System.out.println(String.format("%s: %s times %s ms", name, count, time));
		}

	}

	private int findAddr(int fileNum, byte[] key) {
		int lo = starts[fileNum];
		int hi = lo + allNodes[fileNum] - 1;

		if (QuickSort.compareBytes(keys[lo], key) > 0 || QuickSort.compareBytes(keys[hi], key) < 0)
			return -1;

		// 中间数的下标
		int mid = (lo + hi) / 2;
		boolean found = false;
		while (lo <= hi) { // 退出循环的条件 若一直没找到这个数，则会退出循环
			if (QuickSort.compareBytes(keys[mid], key) == 0) {// 数组中间的数正好是被查找的数直接返回
				found = true;
				break;
			} else if (QuickSort.compareBytes(keys[mid], key) < 0) { //
				lo = mid + 1; // 若小于被查找的数 则证明被查找的数只可能在数组右部分，则将右部分的数组重新进行一次二分查找
			} else {
				hi = mid - 1;// 同理
			}
			mid = (lo + hi) / 2;
		}
		if (!found) {
			return -1;
		} else {
			return addrs[mid];
		}
	}
	

	private void oldRecover() throws Exception {
		int sum = 0;
		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				continue;
			}
			long fileSize = Util.realSize(logFile, Util.VALUE_PAGE);
			final int nodes = (int) (fileSize / Util.SIZE_OF_VALUE);
			System.out.println("i = " + i + ", nodes = " + nodes);
			sum += nodes;
			final long fileNum = (long) i << 48;
			final long initAddr = Util.ADDR_INIT | fileNum;

			File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i));
			try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r"); FileChannel channel = raf.getChannel()) {
				MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
				for (int x = 0; x < nodes; x++) {
					// 获取key
					long keyVal = Util.getBigEndianLong(data);

					long offset = x * 4096l;
					long addrVal = offset | initAddr;
					map.put(keyVal, addrVal);
				}
				Util.ByteBufferSupport.unmap(data);
			}
		}
		System.out.println("sum = " + sum);
	}

	private void oldGroupRecover() throws Exception {
		int sum = 0;
		long s = System.currentTimeMillis();
		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				continue;
			}
			// 记录节点数目
			RandomAccessFile raf4v = new RandomAccessFile(logFile, "r");
			long fileSize = raf4v.length();
			raf4v.close();
			final int nodes = (int) (fileSize / Util.SIZE_OF_VALUE);
			System.out.println("i = " + i + ", nodes = " + nodes);
			allNodes[i] = nodes;

			// 定位起始点
			starts[i] = sum;
			final int start = starts[i];
			// 读取索引
			File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i));
			try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "r"); FileChannel channel = raf.getChannel()) {
				MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
				// 循环读入索引，生成地址
				for (int x = 0; x < nodes; x++) {
					data.get(keys[start + x]);
					addrs[start + x] = x * 4096;
				}

				Util.ByteBufferSupport.unmap(data);
			}
			// 索引排序
			QuickSort.quickSort(keys, addrs, start, start + nodes - 1);
			sum += nodes;
			System.out.println("文件 " + i + " recover耗时： " + (System.currentTimeMillis() - s) + "ms. ");
			s = System.currentTimeMillis();
		}
		System.out.println("sum = " + sum);
	}
}
