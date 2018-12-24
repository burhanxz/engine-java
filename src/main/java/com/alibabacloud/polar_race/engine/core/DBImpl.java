package com.alibabacloud.polar_race.engine.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.FileMetaData;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceComparator;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.core.VLogReader.VLogIterator;

//TODO vlog不能append
//TODO vlog读需要使用fileChannel
//TODO vlog写需要使用大块mappedBytebuffer，当下size太小
//TODO bloomfilter线程池
/*recover时next file number的三种情况
1.第一个memtable尚未写完，则nextFileNum设为1
2.上一个sst未写完，则删除sst，设nextFileNum为此sst的编号
3.上一个sst写完，memtable不是第一个，则无需手动设置*/
public class DBImpl {
	private final ExecutorService readThreadPool;
	private final ExecutorService compactionThreadPool;

	private File databaseDir;
	private final SliceComparator sliceComparator = new SliceComparator();
	private SSDManager manager;
	private ConcurrentSkipListMap<Slice, Slice> memtable;
	private volatile ConcurrentSkipListMap<Slice, Slice> immutableMemtable;
	private VLogBuilder vlog;
	private volatile boolean isFirst;
	private AtomicInteger kvNum = new AtomicInteger(0);
	private TableCache tableCache;
	private VLogReaders vlogReaders;

	// static {
	// }

	public DBImpl(String path) throws IOException, InterruptedException, ExecutionException {
		readThreadPool = Executors.newFixedThreadPool(64, new DBThreadFactory("read thread"));
		compactionThreadPool = Executors.newFixedThreadPool(1, new DBThreadFactory("compaction thread"));

		// 建立db目录
		databaseDir = new File(path);
		if (!databaseDir.exists()) {
			isFirst = true;
			databaseDir.mkdirs();
		}

		// 新建ssdManager
		manager = new SSDManager(databaseDir);
		// 建立memtable，此处建立memtable是为了记录已有的vlog中的数据，在记录完成之前不新建log
		memtable = new ConcurrentSkipListMap<>(sliceComparator);
		// 数据恢复
		if (!isFirst) {
			// 恢复SSDManager信息
			manager.recover();
			// 扫描db文件夹，删除残缺table文件，获取所有vLog文件并且和SSDManager中的信息进行比对
			String[] fileNames = databaseDir.list();
			Collections.sort(Arrays.asList(fileNames));
			// 获取当前所有合法table的文件名，方便比对
			List<String> allTables = new ArrayList<>();
			manager.getFiles().forEach(e -> {
				allTables.add(Util.Filename.tableFileName(e.getNumber()));
			});
			// 获取文件夹中所有的log文件，方便比对
			List<String> unRecordedLogNames = new ArrayList<>();

			// 遍历所有文件
			for (int i = 0; i < fileNames.length; i++) {
				String fileName = fileNames[i];
				// 添加未记录在manager中的log文件
				if (fileName.endsWith("log")) {
					int fileNum = Integer.valueOf(fileName.split("\\.")[0]);
					if (!manager.getvLogNums().contains(fileNum)) {
						unRecordedLogNames.add(fileName);
					}
				}
				// 删除非法table文件
				else if (fileName.endsWith(".sst")) {
					if (!allTables.contains(fileName)) {
						File invalidTableFile = new File(databaseDir, fileName);
						invalidTableFile.delete();
						// 设置最新的next file number
						int fileNum = Integer.valueOf(fileName.split("\\.")[0]);
						manager.setNextFileNum(fileNum);
					}
				}
			}
			// 将log文件按字典序升序排序，新文件在后，旧文件在前
			Collections.sort(unRecordedLogNames);
			// 只需恢复memtable
			if (unRecordedLogNames.size() == 1) {
				String fileName = unRecordedLogNames.get(0);
				// 设置最新的next file number
				int fileNum = Integer.valueOf(fileName.split("\\.")[0]);
				if (fileNum == 0) {
					manager.setNextFileNum(1);
				}
				log2Memtable(fileName);
			}
			// 恢复memtable，immutableMemtable并compaction为table文件
			else if (unRecordedLogNames.size() == 2) {
				// 对于immutable
				log2Table(unRecordedLogNames.get(0));
				// 对于memtable
				log2Memtable(unRecordedLogNames.get(1));
			} else {
				throw new RuntimeException("未知log情形,未记录log数目: " + unRecordedLogNames.size());
			}
		}

		// 建立vlog
		newVLog();
		// 建立tableCache
		tableCache = new TableCache(databaseDir, 1 << 9);
		// 建立vlogreaders
		vlogReaders = new VLogReaders(databaseDir, 1 << 9);
	}

	// 写入
	public void write(Slice key, Slice value) throws IOException {
		System.out.println("输入key = " + Util.bytesToLong(key.getBytes()));
		// 分配空间
		ensureRoom();
		// 首先写入日志
		Slice addr = vlog.addRecord(key, value);
		// 写入跳表
		memtable.put(key, addr);
		// 写入key cache
		KeyCache.resultCache.put(key, addr);
	}

	public Slice read(Slice key) throws InterruptedException, ExecutionException {
		Slice ret = null;
		// 读memtable
		ret = memtable.get(key);
		if (ret != null) {
			System.out.print("获取于memtable ");
			return vlogReaders.getValue(ret);
		}

		// 读immutableMemtable
		if (immutableMemtable != null) {
			ret = immutableMemtable.get(key);
			if (ret != null) {
				System.out.print("获取于immutable memtable ");
				return vlogReaders.getValue(ret);
			}
		}
		// 读resultCache
		ret = KeyCache.resultCache.get(key);
		if (ret != null) {
			System.out.print("获取于resultCache ");
			return vlogReaders.getValue(ret);
		}
		// 进行读table任务
		Future<Slice> future = readThreadPool.submit(new TableReadTask(key));
		System.out.print("获取于read task ");
		return vlogReaders.getValue(future.get());
	}

	public void close() {
		// 做最后一次compaction
		// if (memtable.size() > 0) {
		// // 循环等待immutable memtable为空，即上一次compaction完毕
		// while (true) {
		// if (immutableMemtable == null) {
		// break;
		// }
		// }
		// // 记录memtable和当前log
		// immutableMemtable = memtable;
		// int currentVLogNum = vlog.getFileNumber();
		//
		// // 开一条线程同步处理以下操作
		// Future future = compactionThreadPool.submit(new
		// CompactionTask(currentVLogNum));
		// // 同步阻塞任务处理
		// try {
		// future.get();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// } catch (ExecutionException e) {
		// e.printStackTrace();
		// }
		// // 重置kv总数
		// kvNum.set(1);
		// }

		try {
			vlog.close(); // 关闭VLogBuilder
			manager.close(); // 关闭ssdManager
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 关闭读线程池
		readThreadPool.shutdown();
		try {
			readThreadPool.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		// 关闭写线程池
		compactionThreadPool.shutdown();
		try {
			compactionThreadPool.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		//关闭tableBuilder
		TableBuilder.close();
		// 关闭tableCache
		tableCache.close();
		// 关闭vLogReaders
		vlogReaders.close();
	}

	// 恢复log数据到memtable中去
	private void log2Memtable(String logName) throws IOException {
		kvNum.set(0);
		// 获取log的file number
		int currentVLogNum = Integer.valueOf(logName.split("\\.")[0]);
		File logFile = new File(databaseDir, logName);
		try (FileInputStream vlogFis = new FileInputStream(logFile); FileChannel vLogChannel = vlogFis.getChannel()) {
			VLogReader reader = new VLogReader(vLogChannel);
			VLogIterator it = reader.iterator();
			// 顺序读取vlog
			while (it.hasNext()) {
				// 获取addr
				Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
				SliceOutput addrOut = addr.output();
				addrOut.writeInt(currentVLogNum);
				addrOut.writeInt(it.offset());
				// 获取key
				Slice key = it.next();
//				System.out.println("key: " + Util.bytesToLong(key.getBytes()));

				// k-v添加到跳表中
				memtable.put(key, addr);
				//更新当下kvNum大小
				kvNum.incrementAndGet();
			}
		}

	}

	private void log2Table(String logName) throws FileNotFoundException, IOException, InterruptedException, ExecutionException {
		// 获取log的file number
		int currentVLogNum = Integer.valueOf(logName.split("\\.")[0]);
		// 获取immutableMemtable
		if (immutableMemtable == null) {
			immutableMemtable = new ConcurrentSkipListMap<>(sliceComparator);
		}
		// 获取log文件
		File logFile = new File(databaseDir, logName);
		// 从log获取k-v并加入到immutableMemtable中
		try (FileInputStream vlogFis = new FileInputStream(logFile); FileChannel vLogChannel = vlogFis.getChannel()) {
			VLogReader reader = new VLogReader(vLogChannel);
			VLogIterator it = reader.iterator();
			// 顺序读取vlog
			while (it.hasNext()) {

				// 获取addr
				Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
				SliceOutput addrOut = addr.output();
				addrOut.writeInt(currentVLogNum);
				addrOut.writeInt(it.offset());
				// 获取key
				Slice key = it.next();
//				System.out.println("key: " + Util.bytesToLong(key.getBytes()));

				// k-v添加到跳表中
				immutableMemtable.put(key, addr);
			}
		}

		// 新建table文件
		int tableNum = manager.getNextFileNum();
		String tableFileName = Util.Filename.tableFileName(tableNum);
		File tableFile = new File(databaseDir, tableFileName);
		// 打开table文件
		try (FileOutputStream fos = new FileOutputStream(tableFile); FileChannel channel = fos.getChannel()) {
			TableBuilder tb = new TableBuilder(channel);
			// 将跳表中的数据放入table中
			Set<Slice> keySet = immutableMemtable.keySet();
			Iterator<Slice> it = keySet.iterator();

			// 最值
			Slice smallestKey = null;
			Slice largestKey = null;
			// 遍历跳表
			while (it.hasNext()) {
				Slice key = it.next();
				Slice addr = immutableMemtable.get(key);
				tb.add(key, addr);
				// 更新最大最小key
				if (smallestKey == null) {
					smallestKey = key;
				}
				largestKey = key;
			}
			tb.finish();
			long tableSize = channel.position();
			// 测试
			System.out.println("tableSize = " + tableSize);
			System.out.println("tableSize == Util.TABLE_SIZE? " + ((int) tableSize == Util.TABLE_SIZE));
			// 生成edit信息，apply到manager
			FileMetaData tableFileMetaData = new FileMetaData(tableNum, (int) tableSize, smallestKey, largestKey);
			SSDManagerEdit edit = new SSDManagerEdit(currentVLogNum, tableFileMetaData, tableNum + 1);
			manager.apply(edit);
		}
		// 重置immutableMemtable
		immutableMemtable = null;

	}

	// 新建vlog
	private void newVLog() throws IOException {
		int vLogNum = manager.getNextFileNum();
		String vLogFileName = Util.Filename.logFileName(vLogNum);
		File vLogFile = new File(databaseDir, vLogFileName);
		vlog = new VLogBuilder(vLogFile, vLogNum);
	}

	private synchronized void ensureRoom() throws IOException {
		// 资源足够
		// 根据已有的数目判断
		if (kvNum.get() < Util.MEMTABLE_SIZE) {
			kvNum.incrementAndGet();
			return;
		}
		System.out.println("kvNum临界值： " + kvNum.get());
		System.out.println("memtable临界大小： " + memtable.size());
		// 资源不足
		// 循环等待immutable memtable为空，即上一次compaction完毕
		while (true) {
			if (immutableMemtable == null) {
				break;
			}
		}

		// 记录memtable和当前log
		immutableMemtable = memtable;
		int currentVLogNum = vlog.getFileNumber();
		// 新建memtable和log
		memtable = new ConcurrentSkipListMap<>(sliceComparator);
		newVLog();

		// 开一条线程异步处理以下操作
		compactionThreadPool.submit(new CompactionTask(currentVLogNum));

		// 重置kv总数
		kvNum.set(1);
	}

	// compaction任务
	public class CompactionTask implements Runnable {
		// immutable memtable对应的日志编号
		private int currentVLogNum;

		public CompactionTask(int currentVLogNum) {
			this.currentVLogNum = currentVLogNum;
		}

		@Override
		public void run() {
			try {
				long s = System.currentTimeMillis();
				// 新建table文件
				int tableNum = manager.getNextFileNum();
				String tableFileName = Util.Filename.tableFileName(tableNum);
				File tableFile = new File(databaseDir, tableFileName);
				// 打开table文件
				try (FileOutputStream fos = new FileOutputStream(tableFile); FileChannel channel = fos.getChannel()) {
					TableBuilder tb = new TableBuilder(channel);
					// 将跳表中的数据放入table中
					Set<Slice> keySet = immutableMemtable.keySet();
					Iterator<Slice> it = keySet.iterator();
					// 最值
					Slice smallestKey = null;
					Slice largestKey = null;
					// 遍历跳表
					while (it.hasNext()) {
						Slice key = it.next();
						Slice addr = immutableMemtable.get(key);
						tb.add(key, addr);
						// 更新最大最小key
						if (smallestKey == null) {
							smallestKey = key;
						}
						largestKey = key;
					}
					tb.finish();
					long tableSize = channel.position();
					// 测试
					System.out.println("tableSize = " + tableSize);
					System.out.println("tableFile = " + tableFile.getName());
					// 生成edit信息，apply到manager
					FileMetaData tableFileMetaData = new FileMetaData(tableNum, (int) tableSize, smallestKey,
							largestKey);
					SSDManagerEdit edit = new SSDManagerEdit(currentVLogNum, tableFileMetaData, tableNum + 1);
					manager.apply(edit);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				long e = System.currentTimeMillis();
				System.out.println("写满此单元table耗费时间: " + (e-s) + "ms.");
				// 清空immutableMemtable
				immutableMemtable = null;

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	// 线程工厂，处理异常，设置线程名称
	public static class DBThreadFactory implements ThreadFactory {
		private final AtomicInteger threadCount = new AtomicInteger(0);
		private String name;

		public DBThreadFactory(String name) {
			this.name = name;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setName(name + "-" + threadCount.getAndIncrement());
			t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
				@Override
				public void uncaughtException(Thread t, Throwable e) {
					System.out.println("thread: " + name + " exception: ");
					System.out.println(e);
				}

			});
			return t;
		}

	}
}
