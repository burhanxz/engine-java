package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;

public class PreMemDB {
//	private static final byte[] zeros = new byte[1<<12];
	// private ExecutorService mapPool = Executors.newFixedThreadPool(1);
	private static final int BASE_NUM = Util.LOG_NUM;
	private static final int KEYS_PER_MMAP = (1 << 12) / 8;
	private static final long MAX_NODE = 1 << 26;
	private final File databaseDir;
	private boolean isFirst;
	// private final PreLog log;
//	private final DoubleConcurrentLogs logs;
	private volatile ThreadLocalLogs logs;
	// private final PreTable table;
	private PreLogReader reader;
//	private KeyLogReader keyReader; // TODO 先把filechannel全访问一遍
	// private final Finalizer<PreTable> finalizer = new Finalizer<>(1);
	private ConcurrentLongLongHashMap map;
//	private final LongLongHashMap[] maps = new LongLongHashMap[Util.LOG_NUM];
	private AtomicBoolean recovered = new AtomicBoolean(false);
//	private long dbLastNode;
//	private AtomicLong readTime = new AtomicLong(0);
//	private AtomicBoolean firstWrite = new AtomicBoolean(true);
//	private static final int SEGMENTS = 1 << 9;
//	private static final long BASE_SEGMENT = Long.MAX_VALUE / SEGMENTS;
//	private Object[] objects = new Object[SEGMENTS];
	
	public PreMemDB(String path) throws Exception {
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

		// 建立log,reader和table
		// log = new PreMMapLog(databaseDir, lastLogNum);
		// log = new PreAllocatedLog(databaseDir, lastLogNum);
		// logs = new ConcurrentLogs(databaseDir);
		logs = new ThreadLocalLogs(databaseDir);
		// table = new PreSimpleTable(databaseDir);
		// table = new PreSegmentTable(databaseDir);
		// table = new PreLinkedListTable(databaseDir);
		reader = new PreLogReader(databaseDir, 1000);
//		keyReader = new KeyLogReader(databaseDir, 1000);
		
//		long lastNode = 0;
//		long lastOffset = 0;
//		for (int i = 0; i < BASE_NUM; i++) {
////			lastOffset += reader.getChannel(i).size();
//			lastOffset += Util.realSize(reader.getChannel(i), Util.VALUE_PAGE);
//		}
//		System.out.println("double lastNode = " + (lastOffset * 1.0 / (1 << 12)));
//		lastNode = lastOffset / (1 << 12);
//		System.out.println("lastNode = " + lastNode);
//		this.dbLastNode = lastNode;
		// 建立hashtable
//		map = new LongLongHashMap(Util.MAP_SIZE, 0.99);
		map = new ConcurrentLongLongHashMap(Util.MAP_SIZE, 0.99);
//		for(int i = 0; i < SEGMENTS; i++) {
//			objects[i] = new Object();
//		}
//		map = null;
		// 恢复数据
		if (!isFirst) {
//			recover();
			parallelRecover();
		}
//		logs.setStart(lastNode);

	}

	public void write(byte[] key, byte[] value) throws Exception {
//		if(logs == null) {
//			synchronized(this) {
//				if(logs == null) {
//					logs = new ThreadLocalLogs(databaseDir);
//				}
//			}
//		}
//		System.out.println("key = " + new Slice(key).getLong(0));
//		if (recovered.get() == true) {
//			synchronized (this) {
//				if (recovered.get() == true)
//					recovered.set(false);
//			}
//		}
//		if (firstWrite.get() == true) {
//			synchronized (this) {
//				if (firstWrite.get() == true) {
//					firstWrite.set(false);
//					System.out.println(">>>>>>>>第一次写<<<<<<<<");
//				}
//					
//			}
//		}
		// 写value
		// Slice addr = log.add(value);
		/* long addrVal = */logs.add(key, value);
		// 测试
		// int fileNum = addr.getInt(0);
		// int fileOffset = addr.getInt(Util.SIZE_OF_INT);
		// System.out.print("fileNum = " + fileNum);
		// System.out.println(" fileOffset = " + fileOffset);
		// 写key-addr
		// table.add(key, addr.getRawArray());
		// long keyVal = new Slice(key).getLong(0);
	}

	public byte[] read(byte[] key) throws Exception {
		//TODO 考虑去除
//		if (recovered.get() == false) {
//			parallelRecover();
//		}
		// 并发读table
		//TODO 可优化
//		long keyVal = new Slice(key).getLong(0);
		long keyVal = Util.getLittleEndianLong(key);
//		long addrVal = 0l;
//		for(int i = 0; i < BASE_NUM; i++) {
//			addrVal = maps[i].get(keyVal);
//			if(addrVal != 0l)
//				break;
//		}
		long addrVal = map.get(keyVal); //TODO 已经消除0key
		if(addrVal == 0l) {
			return null;
//			if(!map.containsKey(keyVal)) {
//				return null;
//			}
		}
		// fileChannel读log
//		Slice addr = Slices.allocate(8);
//		addr.setLong(0, addrVal);
		byte[] value = reader.getValue(addrVal);
//		long x = 0;
//		if((x = readTime.getAndIncrement()) % 10000 == 0) {
//			System.out.print("readTime = " + x + ", ");
//		}
		return value;
	}

	public synchronized void close() throws Exception {
		// log.close();
		logs.close();
		reader.close();
//		keyReader.close();
		Works.close();
		// finalizer.addCleanup(table, table.closer());
		// finalizer.destroy();

		System.out.println("关闭：" + databaseDir.getAbsolutePath());
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            long time = gc.getCollectionTime();
            String name = gc.getName();
            System.out.println(String.format("%s: %s times %s ms", name, count, time));
        }
//		for (File file : databaseDir.listFiles()) {
//			System.out.println(file.getName());
////			Files.delete(file.toPath());
//		}
	}

	public synchronized void oldRecover() {
		throw new UnsupportedOperationException();
//		// 双重检查
//		if (recovered.get() == false) {
//			map.clear();
//			long s = System.currentTimeMillis();
//			IntStream.range(0, (1 << 8)).forEach(index -> {
//				File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(index));
//				File logFile = new File(databaseDir, Util.Filename.logFileName(index));
//				try (RandomAccessFile raf = new RandomAccessFile(kLogFile, "rw");
//						RandomAccessFile raf2 = new RandomAccessFile(logFile, "rw");
//						FileChannel channel = raf.getChannel()) {
//					// 对应的value log的最大offset
//					long maxOffset = raf2.length() - (1 << 12);
//					long len = raf.length();
//					boolean isEnd = false;
//					for (int i = 0; i < len; i += (1 << 12)) {
//						MappedByteBuffer buff = channel.map(MapMode.READ_WRITE, i, (1 << 12));
//
//						Slice keys = Slices.copiedBuffer(buff, 0, (1 << 12));
//						for (int j = 0; j < (1 << 12); j += 8) {
//							Slice data = keys.slice(j, 8);
//							long keyVal = data.getLong(0);
//
//							// 算出对应的value log的offset
//							int offset = (int) ((i + j) * (1 << 9));
//							if (offset > maxOffset) {
//								isEnd = true;
//								break;
//							}
//
//							Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
//							SliceOutput output = addr.output();
//							output.writeInt(index);
//							output.writeInt(offset);
//							long addrVal = addr.getLong(0);
//							// 测试
//							// System.out.println("keyVal = " + keyVal + ", addrVal = " + addrVal);
//							// System.out.println("fileNum = " + addr.getInt(0) + ", offset = " +
//							// addr.getInt(4));
//
//							map.put(keyVal, addrVal);
//
//						}
//
//						Util.ByteBufferSupport.unmap(buff);
//						if (isEnd) {
//							return;
//						}
//					}
//
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			});
//			recovered.set(true);
//			long e = System.currentTimeMillis();
//			System.out.println("recover耗时: " + (e - s) + "ms.");
//		}

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
//					if(data == null) {
//						System.out.println("fileNum = " + i);
//					}
//					Objects.requireNonNull(data);
					for(int x = 0; x < nodes; x++) {
						//获取key
						long keyVal = Util.getLittleEndianLong(data);
//						if(map.containsKey(keyVal)) {
//							System.out.print(keyVal + ", ");
//						}
//						byte[] key = new byte[8];
//						data.get(key, 0, 8);
//						long keyVal = new Slice(key).getLong(0);
//						System.out.println("key = " + Util.bytesToLong(key));
						//获取addr
//						Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
						long offset = x * 4096l;
						long addrVal =  offset | initAddr;
//						System.out.println("key = " + keyVal);
						map.put(keyVal, addrVal);
					}
					
//					Util.ByteBufferSupport.unmap(data);
					Works.unmap(data);
				}
			}

			recovered.set(true);
			long e = System.currentTimeMillis();
			System.out.println("recover耗时: " + (e - s) + "ms.");
		}
		return 0l;
	}
	
//	private Object segmentFor(long key) {
//		key = (key < 0) ? -key : key;
//		int segment = (int) (key / BASE_SEGMENT);
//		if(segment >= SEGMENTS)
//			segment = SEGMENTS - 1;
//		return objects[segment];
//	}
	
	private void doParallelReover(final int fileNumer, final int nodes, final CountDownLatch latch) {
		File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(fileNumer));
		try(RandomAccessFile raf = new RandomAccessFile(kLogFile, "rw");
				FileChannel channel = raf.getChannel()){
			final long fileNum = (long)fileNumer << 48;
			final long initAddr = Util.ADDR_INIT | fileNum;
			MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
			for(int x = 0; x < nodes; x++) {
				long keyVal = Util.getLittleEndianLong(data);
				long offset = x * 4096l;
				long addrVal = initAddr | offset;
				map.syncPut(keyVal, addrVal);
			}
			
			Util.ByteBufferSupport.unmap(data);
			latch.countDown();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public long parallelRecover() throws Exception {
		if (recovered.get() == false) {
			long s = System.currentTimeMillis();
//			reader = new PreLogReader(databaseDir, 1000);
//			keyReader = new KeyLogReader(databaseDir, 1000);
//			map = new ConcurrentLongLongHashMap(Util.MAP_SIZE, 0.99);
//			if(map.size() != 0) {
//				System.out.println("map.size = " + map.size());
//				map.clear();
//			}
			int[] fileNodes = new int[BASE_NUM];
//			Map<Integer, Integer> fileNodes = new HashMap<>();
			int sum = 0;
			for (int i = 0; i < BASE_NUM; i++) {
				long len = Util.realSize(reader.getChannel(i), Util.VALUE_PAGE);
				int nodes = (int) (len / (1<<12));
				fileNodes[i] = nodes;
				sum += nodes;
			}
			System.out.println("node sum = " + sum);
			final CountDownLatch latch = new CountDownLatch(BASE_NUM);
			for(int n = 0; n < BASE_NUM; n++) {
				final int i = n;
				Works.getPool().execute( () -> {
					doParallelReover(i, fileNodes[i], latch);
//					File kLogFile = new File(databaseDir, Util.Filename.keyLogFileName(i));
//					try(RandomAccessFile raf = new RandomAccessFile(kLogFile, "rw");
//							FileChannel channel = raf.getChannel()){
//						final int nodes = fileNodes[i];
//						final long fileNum = (long)i << 48;
//						final long initAddr = Util.ADDR_INIT | fileNum;
////						maps[i] = new LongLongHashMap(nodes, 0.99);
////						System.out.println("nodes = " + nodes);
//						MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0, nodes * 8);
////						if(data == null) {
////							System.out.println("fileNum = " + i);
////						}
////						Objects.requireNonNull(data);
//						for(int x = 0; x < nodes; x++) {
//							//获取key
//							long keyVal = Util.getLittleEndianLong(data);
////							if(map.containsKey(keyVal)) {
////								System.out.print(keyVal + ", ");
////							}
////							byte[] key = new byte[8];
////							data.get(key, 0, 8);
////							long keyVal = new Slice(key).getLong(0);
////							System.out.println("key = " + Util.bytesToLong(key));
//							//获取addr
////							Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
//							long offset = x * 4096l;
//							long addrVal = initAddr | offset;
////							System.out.println("key = " + keyVal);
//							map.syncPut(keyVal, addrVal);
//						}
//						
//						Util.ByteBufferSupport.unmap(data);
////						MMaps.unmap(data);
//						latch.countDown();
//					} catch (Exception e1) {
//						// TODO Auto-generated catch block
//						e1.printStackTrace();
//					}
				} );

			}
			latch.await();
			recovered.set(true);
			long e = System.currentTimeMillis();
			System.out.println("recover耗时: " + (e - s) + "ms.");
		}
		return 0l;
	}
	
	public synchronized long recover(long lastNode) throws Exception {
		throw new UnsupportedOperationException();
	}
//		if (recovered.get() == false) {
//			long s = System.currentTimeMillis();
//			if(lastNode == 0) {
//				long lastOffset = 0;
//				for (int i = 0; i < BASE_NUM; i++) {
////					lastOffset += reader.getChannel(i).size();
//					lastOffset += Util.realSize(reader.getChannel(i), Util.VALUE_PAGE);
//				}
//				lastNode = lastOffset / (1 << 12);
//				System.out.println("lastNode = " + lastNode);
//			}
//			Map<Integer, MappedByteBuffer> bufferMap = new HashMap<>();
//			for(long i = 0; i < lastNode; i++) {
//				int node = (int) (i / BASE_NUM);
//				int fileNum = (int) (i % BASE_NUM);
//				
//				//获取mmap
//				MappedByteBuffer buffer = bufferMap.get(fileNum);
//				if(buffer == null) {
//					if(node != 0) {
//						System.out.println("i = " + i);
//						System.out.println("node = " + node);
//						throw new RuntimeException("node 数量错误");
//					}
//					int newPosition = node * 8;
//					long end = keyReader.getChannel(fileNum).size();
//					int len = 1<<12;
//					if(end - newPosition < (1<<12)) {
//						len = (int) (end - newPosition);
//					}
//					buffer = keyReader.getChannel(fileNum).map(MapMode.READ_ONLY, 0, len);
//					bufferMap.put(fileNum, buffer);
//				}
//				else if(buffer.position() == (1<<12)) {
//					if(node % 512 != 0) {
//						System.out.println("i = " + i);
//						System.out.println("node = " + node);
//						throw new RuntimeException("node 数量错误");
//					}
//					//舍弃
//					Util.ByteBufferSupport.unmap(buffer);
//					//新建
//					int newPosition = node * 8;
//					long end = keyReader.getChannel(fileNum).size();
//					int len = 1<<12;
//					if(end - newPosition < (1<<12)) {
//						len = (int) (end - newPosition);
//					}
//					buffer = keyReader.getChannel(fileNum).map(MapMode.READ_ONLY, newPosition, len);
//					bufferMap.put(fileNum, buffer);
//				}
//				
//				//读mmap
//				//获取key
//				byte[] key = new byte[8];
//				buffer.get(key, 0, 8);
//				long keyVal = new Slice(key).getLong(0);
//				//获取addr
//				Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
//				SliceOutput output = addr.output();
//				output.writeInt(fileNum);
//				output.writeInt((int)(node * (1<<12)));
//				long addrVal = addr.getLong(0);
//				
////				Long oldAddr = map.get(keyVal);
////				if(oldAddr != null) {
////					System.out.println("重复key = " + );
////				}
//				map.put(keyVal, addrVal);
//			}
//			recovered.set(true);
//			long e = System.currentTimeMillis();
//			System.out.println("recover耗时: " + (e - s) + "ms.");
//		}
//		return lastNode;
//	}
}
