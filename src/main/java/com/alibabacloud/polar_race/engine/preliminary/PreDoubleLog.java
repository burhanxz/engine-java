package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;

public class PreDoubleLog {
	// private static ExecutorService pool = Executors.newFixedThreadPool(2);
	private static int NEW_DATA_POS = Util.VALUE_PAGE / 2;
	private static long KEY_LOG_SIZE = 1 << 23; // key file最大8M
	private static long LOG_FILE_SIZE = (1 << 30) + (1 << 22);// value file 最大1G + 4M
	// private static long LOG_FILE_SIZE = 1 << 20;//测试用1M
	private File databaseDir;
	private FileChannel fileChannel;
	private MappedByteBuffer data;

	// private Future<MappedByteBuffer> newData;
	private long offset;
	private final int fileNum;
	private final PreKeyLog klog;
	private int lastNode;

	public PreDoubleLog(File databaseDir, int fileNum) throws Exception {
		this.databaseDir = databaseDir;
		this.fileNum = fileNum;
		// 获取文件名
		String fileName = Util.Filename.logFileName(fileNum);

		// 获取文件
		File log = new File(databaseDir, fileName);

		if (!log.exists())
			log.createNewFile();

		RandomAccessFile raf = new RandomAccessFile(log, "rw");

		this.fileChannel = raf.getChannel();
		this.offset = Util.realSize(fileChannel, Util.VALUE_PAGE);
		this.lastNode = (int) (offset / (1 << 12));
		// System.out.println("last");
		this.klog = new PreKeyLog();
		this.data = fileChannel.map(MapMode.READ_WRITE, offset, Util.VALUE_PAGE);
		// System.out.println("log编号 = " + fileNum + ", 初始offset = " + offset);
	}

	public synchronized void add(byte[] key, byte[] value) throws Exception {
		add(value);
		klog.add(key);
	}

	public void add(byte[] value) throws Exception {
		ensureRoom();
		// value写入mmap
		data.put(value);
		// System.out.println("remaining = " + data.remaining());
		// 更新offset并回收mmap
		// TODO 可优化
		// offset += (1<<12);

	}

	public synchronized void ensureRoom() throws Exception {
		if (data.position() == Util.VALUE_PAGE) {
			Util.ByteBufferSupport.unmap(data);
			// Works.unmap(data);
			offset += Util.VALUE_PAGE;
			// TODO 异步分配mmap
			data = fileChannel.map(MapMode.READ_WRITE, offset, Util.VALUE_PAGE);
			// data = newData.get();
		}
		// } else if (data.position() == NEW_DATA_POS) {
		// long newOffset = offset + Util.VALUE_PAGE;
		// newData = Works.map(fileChannel, newOffset, Util.VALUE_PAGE);
		// }
	}

	public synchronized boolean hasRoom() throws IOException {
		// 超过规定大小
		if (this.offset >= LOG_FILE_SIZE) {
			// System.out.println("到达临界offset = " + offset);
			// 关闭channel
			return false;
		}
		return true;
	}

	// 正确关闭
	public synchronized void close() throws IOException {
		if (fileChannel != null && !fileChannel.isOpen()) {
			fileChannel.close();
		}
		// Util.Closeables.closeQuietly(fileChannel);
	}

	public long getOffset() {
		return offset;
	}

	public int getFileNum() {
		return fileNum;
	}

	public class PreKeyLog {
		private final static int NEW_DATA_POS_4K = Util.KEY_PAGE / 2;
		private final static int KEY_PAGE_SIZE = Util.KEY_PAGE; // 1MB
		private FileChannel fileChannel4k;
		private MappedByteBuffer data4k;
		private Future<MappedByteBuffer> newData4k;
		private long offset4k;

		public PreKeyLog() throws IOException {
			// 获取文件名
			String fileName = Util.Filename.keyLogFileName(fileNum);
			// 获取文件
			File log4k = new File(databaseDir, fileName);
			if (!log4k.exists())
				log4k.createNewFile();

			RandomAccessFile raf = new RandomAccessFile(log4k, "rw");
			this.fileChannel4k = raf.getChannel();
			this.offset4k = lastNode * 8;
			this.data4k = fileChannel4k.map(MapMode.READ_WRITE, offset4k, KEY_PAGE_SIZE);

		}

		public void add(byte[] key) throws Exception {
			ensurePage();
			data4k.put(key);
			// TODO 可优化
			// offset4k += 8;
		}

		public void ensurePage() throws Exception {
			// 越界则更换mmap
			if (data4k.position() == KEY_PAGE_SIZE) {
				// data4k.force();
				// Util.ByteBufferSupport.unmap(data4k);
				Works.unmap(data4k);
				offset4k += Util.KEY_PAGE;
				// data4k = fileChannel4k.map(MapMode.READ_WRITE, offset4k, KEY_PAGE_SIZE);
				data4k = newData4k.get();
			} else if (data4k.position() == NEW_DATA_POS_4K) {
				long newOffset = offset4k + Util.KEY_PAGE;
				newData4k = Works.map(fileChannel4k, newOffset, Util.KEY_PAGE);
			}
		}
	}

}
