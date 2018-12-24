package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.alibabacloud.polar_race.engine.base.Util;

public class DoubleConcurrentLogs {
//	private final FileChannel numChannel;
	//TODO 待更新
	private AtomicLong num = new AtomicLong(0l);
	private static final int LOGS_NUM = Util.LOG_NUM;
//	private final Queue<PreDoubleLog> q4Close = new LinkedList<>();
//	private final BlockingQueue<PreDoubleLog> bq = new ArrayBlockingQueue<>(LOGS_NUM + 1);
	private final Map<Integer, PreDoubleLog> logMap = new HashMap<>(LOGS_NUM + 1);
	private final File databaseDir;

	public DoubleConcurrentLogs(File databaseDir) throws Exception {
		this.databaseDir = databaseDir;
//		File numFile = new File(databaseDir, "num");
//		if(!numFile.exists()) {
//			numFile.createNewFile();
//		}
//		this.numChannel = new RandomAccessFile(numFile, "rw");
		// 放入64个log写入器
		for (int i = 0; i < LOGS_NUM; i++) {
			PreDoubleLog log = new PreDoubleLog(databaseDir, i);
			logMap.put(i, log);
		}
	}
	
//	public void oldAdd(byte[] key, byte[] value) throws InterruptedException, IOException {
//		PreDoubleLog log = null;
//				
//		// 获取可用log
//		log = bq.take();
//		//实际上在获取log时就可以获取到addr实际位置了，即预分配
//		int fileNum = log.getFileNum();
//		int offset = (int) log.getOffset();
//
////		Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
////		SliceOutput output = addr.output();
////		output.writeInt(fileNum);
////		output.writeInt((int)offset);
////		long addrVal = addr.getLong(0);
//
//		log.add(key, value);
//		//如果log有剩余空间则放回队列
//		if (log.hasRoom()) {
//			bq.put(log);
//		}
//		//空间已满则放入回收队列
//		else {
//			q4Close.add(log);
//		}
////		return addrVal;
//	}
	
	public void add(byte[] key, byte[] value) throws Exception {
		long n = num.getAndIncrement();
		//实际上在获取log时就可以获取到addr实际位置了，即预分配
		int fileNum = (int) (n % LOGS_NUM);
		//测试数据
//		int node = (int) (n / LOGS_NUM);
//		int offset = node * (1 << 12);
//		System.out.println("(1)fileNum = " + fileNum + ", offset = " + offset);

		// 获取可用log
		PreDoubleLog log = logMap.get(fileNum);
//		System.out.println("(2)fileNum = " + log.getFileNum() + ", offset = " + log.getOffset());

		log.add(key, value);
	}

	public void setStart(long n) {
		num.set(n);
	}
	
//	public void writeNum(long n) throws Exception {
//		FileLock fileLock = null;
//		try {
//			fileLock = numChannel.tryLock();
//			numChannel.write(ByteBuffer.wrap(Util.longToBytes(n)));
//		} finally {
//			fileLock.release();
//		}
//	}
	
	public void close() {
		logMap.forEach((k,v) -> {
			try {
				v.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}
}
