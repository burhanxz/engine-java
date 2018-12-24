package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.preliminary.Works;
import com.alibabacloud.polar_race.engine.preliminary.PreLogReader;
import com.alibabacloud.polar_race.engine.preliminary.ThreadLocalLogs;

public class DB {
	private static final int BASE_NUM = Util.LOG_NUM;
	private static final int KEYS_PER_MMAP = (1 << 12) / 8;
	private static final long MAX_NODE = 1 << 26;
	private final File databaseDir;
	private boolean isFirst;
//	private volatile ThreadLocalLogs logs;
	private volatile SlicedLogs logs;
//	private PreLogReader reader;
	private SlicedLogReader reader;
	private SlicedDIOLogReader dioReader;
	private SortableLongLongHashMap map;
	private SortableLongLongHashMap.ArrayReader arrayReader;
//	private SortableLongLongHashMap.MemoryReader memReader;
	private AtomicBoolean recovered = new AtomicBoolean(false);
//	private Registry registry = null;
//	private DIORegistry registry = null;
//	private volatile ConcurrentRegistry registry = null;
	private volatile SlicedConcurrentRegistry registry = null;
//	private volatile SlicedDIOConcurrentRegistry registry = null;
//	private volatile NoDelayRegistry registry = null;
	private volatile CountDownLatch latch = null;
	public DB(String path) throws Exception {

		System.out.println("open: " + path);
		// 建立db目录
		databaseDir = new File(path);
		if (!databaseDir.exists()) {
			isFirst = true;
			databaseDir.mkdirs();
			System.out.println("初始化db目录 = " + path);
		}
		else if(databaseDir.listFiles() == null || databaseDir.listFiles().length == 0) {
			isFirst = true;
			System.out.println("初始化db目录 = " + path);
		}

		FileManager.init(databaseDir);
//		logs = new ThreadLocalLogs(databaseDir);
		logs = new SlicedLogs(databaseDir);
//		reader = new PreLogReader(databaseDir, 1000);

		// 恢复数据
		if (!isFirst) {
			reader = new SlicedLogReader(databaseDir, 1000);
			dioReader = new SlicedDIOLogReader(databaseDir);
			map = new SortableLongLongHashMap(Util.MAP_SIZE, 0.99);

			slicedRecover();
			System.gc();
//			parallelRecover();
//			registry = new DIORegistry(databaseDir, arrayReader, map);
//			registry = new Registry(databaseDir, arrayReader, map);
			printJVM();
		}
		
//		Works.getPool().execute(() -> {
//			try {
//				Thread.sleep(1000 * 60 * 5);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			System.out.println("5min 超时退出");
//			System.exit(-1);
//		});
//		
	}

	public void write(byte[] key, byte[] value) throws Exception {

		logs.add(key, value);

	}

	public byte[] read(byte[] key) throws Exception {
//		long keyVal = Util.getLittleEndianLong(key);
		long keyVal = Util.getBigEndianLong(key);
		long addrVal = map.get(keyVal); //TODO 已经消除0key
		if(addrVal == 0l) {
			return null;
		}
		byte[] value = dioReader.getValue(addrVal);
		return value;
	}
	
	public void range2(byte[] lower, byte[] upper, AbstractVisitor visitor) throws Exception {
		throw new UnsupportedOperationException();
//		Thread.sleep((threads.incrementAndGet() % 64) * 5);
//		
//		boolean hasLower = lower == null ? false : true;
//		boolean hasUpper = upper == null ? false : true;
//		long lowerVal = hasLower ? Util.getBigEndianLong(lower) : 0;
//		long upperVal = hasUpper ? Util.getBigEndianLong(upper) : 0;
//		System.out.println("hasLower = " + hasLower + ", hasUpper = " + hasUpper);
//		System.out.println("lowerVal = " + lowerVal + ", upperVal = " + upperVal);
//		Iterator<Long> it = hasLower ? arrayReader.iterator(lowerVal) 
//				: arrayReader.iterator();
//		
//		int x = 0;
//		long s = System.currentTimeMillis();
//		long keyVal = 0;
//		long addrVal = 0;
//		long existing = 0;
//		int slot = 0;
//		for(; it.hasNext();) {
//			keyVal = it.next();
//			if(hasUpper && keyVal >= upperVal) {
//				break;
//			}
//			if(keyVal == 0) {
//				System.out.println("range key = 0 !!!!!!");
//			}
//			byte[] key = encoder.encode(keyVal);
//			//TODO 可优化
////			addrVal = map.get(keyVal);
//			addrVal = map.get(keyVal, slot, existing);
//			byte[] value = rangeReader.getValue(addrVal);
//			visitor.visit(key, value);
//			
//			if(x++ % 1000000 == 0) {
//				System.out.println(Thread.currentThread().getName() + 
//						", x = " + x + ", 耗时: " + (System.currentTimeMillis() - s)
//						+ "ms.");
//				printJVM();
//				s = System.currentTimeMillis();
//			}
//		}
//		
	}
	
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws Exception {
		//TODO 两次调用range都是全量吗
//		boolean hasLower = lower == null ? false : true;
//		boolean hasUpper = upper == null ? false : true;
//		long lowerVal = hasLower ? Util.getBigEndianLong(lower) : 0;
//		long upperVal = hasUpper ? Util.getBigEndianLong(upper) : 0;
//		System.out.println("hasLower = " + hasLower + ", hasUpper = " + hasUpper);
//		System.out.println("lowerVal = " + lowerVal + ", upperVal = " + upperVal);
		// 第一个线程初始化registry
		if(registry == null) {
			synchronized(this) {
				if(registry == null) {
					//串行基数排序
					long s1 = System.currentTimeMillis();
//					memReader = map.getSortedKeys(databaseDir);
					arrayReader = map.getSortedKeys(databaseDir);
					long e1 = System.currentTimeMillis();
					System.out.println("基数排序耗时: " + (e1 - s1) + "ms.");
					registry = new SlicedConcurrentRegistry(databaseDir, arrayReader, map);
//					registry = new NoDelayRegistry(databaseDir, memReader, map);
				}
			}
		}
		
		//第一个线程初始化latch
		if(latch == null) {
			synchronized(this) {
				if(latch == null) {
					latch = new CountDownLatch(64);
				}
			}
		}
		//确保range同时开始
		latch.countDown();
		latch.await();
		//确保所有线程都通过了await()
		Thread.sleep(100);
		if(latch != null) {
			synchronized(this) {
				if(latch != null) {
					latch = null;
				}
			}
		}
		
		//注册
//		State state = registry.register();
		State state = registry.register(visitor);
		//循环访问状态
		for(;;) {
			if(state.getState() == State.WRITABLE) {
				registry.writeCache();
			}
			else if(state.getState() == State.READABLE) {
				registry.readCache(visitor, state);
			}
			else if(state.getState() == State.CLOSABLE) {
				break;
			}
		}
	}
	
	public synchronized void close() throws Exception {

		logs.close();
		if(reader != null) {
			reader.close();
		}
		if(dioReader != null) {
			dioReader.close();
		}
		if(registry != null) {
//			registry.close();
		}
//		registry.close();
		Works.close();

		System.out.println("关闭：" + databaseDir.getAbsolutePath());
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            long time = gc.getCollectionTime();
            String name = gc.getName();
            System.out.println(String.format("%s: %s times %s ms", name, count, time));
        }

	}
	
	public long slicedRecover() throws Exception {
		if (recovered.get() == false) {
			long s = System.currentTimeMillis();
			
			long[][] fileLens = Util.slicedRealSize(reader);
			int[][] fileNodes = new int[64][];
			int sum = 0;
			Map<Integer, List<Integer>> fileList = FileManager.getMap();
			for(int i = 0; i < 64; i++) {
				fileNodes[i] = new int[fileList.get(i).size()];
				for(int j = 0; j < fileNodes[i].length; j++) {
					fileNodes[i][j] = (int) (fileLens[i][j] / Util.SIZE_OF_VALUE);
					sum += fileNodes[i][j];
				}				
			}
			System.out.println("node sum = " + sum);
			
			//并发插入数据
			final CountDownLatch latch = new CountDownLatch(64);
			for(int n = 0; n < 64; n++) {
				final int i = n;
				Works.getPool().execute( () -> {
					doSlicedParallelReover(i, fileNodes[i], latch);
				} );
			}
			latch.await();
//			//串行基数排序
//			long s1 = System.currentTimeMillis();
//			arrayReader = map.getSortedKeys(databaseDir);
//			long e1 = System.currentTimeMillis();
			//结束recover
			recovered.set(true);
			long e = System.currentTimeMillis();
//			System.out.println("基数排序耗时: " + (e1 - s1) + "ms.");
			System.out.println("recover耗时: " + (e - s) + "ms.");
			//TODO
			System.gc();
		}
		return 0l;
	}
	
	private void doSlicedParallelReover(int fileNumber, final int[] nodes, final CountDownLatch latch) {
		File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(fileNumber));
		int nodeSum = 0;
		int[] nodeSums = new int[nodes.length];
		for(int i = 0; i < nodes.length; i++) {
			nodeSum += nodes[i];
			nodeSums[i] = nodeSum;
		}
		try(RandomAccessFile raf = new RandomAccessFile(kLogFile, "rw");
				FileChannel channel = raf.getChannel()){
			MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodeSum * 8);
			int nodeSumsNum = 0;
			int nodeOffset = 0;
			for(int x = 0; x < nodeSum; x++) {
				// 更换文件名
				if(x >= nodeSums[nodeSumsNum]) {
					fileNumber += 64;
					nodeSumsNum++;
				}
				nodeOffset = nodeSumsNum > 0 ? x - nodeSums[nodeSumsNum - 1] : x;
				long keyVal = Util.getBigEndianLong(data);
//				if(keyVal == 0) {
//					System.out.println("recover 0 key");
//				}
				long fileNum = (long)fileNumber << 48;
				long initAddr = Util.ADDR_INIT | fileNum;
				long offset = nodeOffset * 4096l;
				long addrVal = initAddr | offset;
				map.syncPut2(keyVal, addrVal);
			}
			Util.ByteBufferSupport.unmap(data);
			latch.countDown();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	public long recover() throws Exception {
		if (recovered.get() == false) {
			long s = System.currentTimeMillis();
			if(map.size() != 0) {
				System.out.println("map.size = " + map.size());
				map.clear();
			}
			int[] fileNodes = new int[BASE_NUM];
			int sum = 0;
			for (int i = 0; i < BASE_NUM; i++) {
				long len = Util.realSize(reader.getChannel(i), Util.VALUE_PAGE);
				int nodes = (int) (len / (1<<12));
				fileNodes[i] = nodes;
				sum += nodes;
			}
			System.out.println("node sum = " + sum);
			for(int i = 0; i < BASE_NUM; i++) {
				File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i));
				try(RandomAccessFile raf = new RandomAccessFile(kLogFile, "rw");
						FileChannel channel = raf.getChannel()){
					final int nodes = fileNodes[i];
					final long fileNum = (long)i << 48;
					final long initAddr = Util.ADDR_INIT | fileNum;
//					System.out.println("nodes = " + nodes);
					MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
					for(int x = 0; x < nodes; x++) {
						//获取key
						long keyVal = Util.getLittleEndianLong(data);

						long offset = x * 4096l;
						long addrVal =  offset | initAddr;
						map.put(keyVal, addrVal);
					}
					
					Works.unmap(data);
				}
			}

			recovered.set(true);
			long e = System.currentTimeMillis();
			System.out.println("recover耗时: " + (e - s) + "ms.");
		}
		return 0l;
	}
	
	private void doParallelReover(final int fileNumer, final int nodes, final CountDownLatch latch) {
		File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(fileNumer));
		try(RandomAccessFile raf = new RandomAccessFile(kLogFile, "rw");
				FileChannel channel = raf.getChannel()){
			final long fileNum = (long)fileNumer << 48;
			final long initAddr = Util.ADDR_INIT | fileNum;
			MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
			for(int x = 0; x < nodes; x++) {
//				long keyVal = Util.getLittleEndianLong(data);
				long keyVal = Util.getBigEndianLong(data);
				if(keyVal == 0) {
					System.out.println("recover 0 key");
				}
				long offset = x * 4096l;
				long addrVal = initAddr | offset;
				map.syncPut2(keyVal, addrVal);
			}
			Util.ByteBufferSupport.unmap(data);
			latch.countDown();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	
	public long parallelRecover() throws Exception {
		if (recovered.get() == false) {
			long s = System.currentTimeMillis();
			int[] fileNodes = new int[BASE_NUM];
			int sum = 0;
			for (int i = 0; i < BASE_NUM; i++) {
				long len = Util.realSize(reader.getChannel(i), Util.VALUE_PAGE);
				int nodes = (int) (len / (1<<12));
				fileNodes[i] = nodes;
				sum += nodes;
			}
			System.out.println("node sum = " + sum);
			//并发插入数据
			final CountDownLatch latch = new CountDownLatch(BASE_NUM);
			for(int n = 0; n < BASE_NUM; n++) {
				final int i = n;
				Works.getPool().execute( () -> {
					doParallelReover(i, fileNodes[i], latch);
				} );
			}
			latch.await();
//			//串行基数排序
//			long s1 = System.currentTimeMillis();
//			arrayReader = map.getSortedKeys(databaseDir);
//			long e1 = System.currentTimeMillis();
			//结束recover
			recovered.set(true);
			long e = System.currentTimeMillis();
//			System.out.println("基数排序耗时: " + (e1 - s1) + "ms.");
			System.out.println("recover耗时: " + (e - s) + "ms.");
			//TODO
			System.gc();
		}
		return 0l;
	}

	private void printJVM() {
		for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
			final String kind = pool.getType() == MemoryType.HEAP ? "heap" : "nonheap";
			final MemoryUsage usage = pool.getUsage();
			if(kind.equals("heap")) {
				System.out.println(Thread.currentThread().getName() + ", [" + pool.getName() + "] init = " + usage.getInit() + ", rate = " + (usage.getUsed() * 1.0f)/usage.getInit());
			}
		}
	}

}
