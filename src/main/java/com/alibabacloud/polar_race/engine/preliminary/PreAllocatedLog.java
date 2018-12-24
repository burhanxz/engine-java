package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class PreAllocatedLog implements PreLog {
	private static int LOG_FILE_SIZE = 1 << 30;
	private static int NODES = LOG_FILE_SIZE / Util.SIZE_OF_VALUE;
	private final Lock mutex = new ReentrantLock();
	private File databaseDir;
	private FileChannel fileChannel;
	private AtomicInteger node;
	private AtomicInteger currentFileNum = new AtomicInteger(0);

	public PreAllocatedLog(File databaseDir, int fileNum) throws IOException {
		this.databaseDir = databaseDir;
		// 获取文件名
		String fileName = Util.Filename.logFileName(fileNum);
		// 获取文件
		File log = new File(databaseDir, fileName);
		if (!log.exists())
			log.createNewFile();

		RandomAccessFile raf = new RandomAccessFile(log, "rw");
		long offset = raf.length();
		this.node = new AtomicInteger((int) offset);
		this.fileChannel = raf.getChannel();

		System.out.println("初始offset = " + offset);
		if (node.get() >= NODES) {
			ensureRoom();
		}
	}
	//线程安全
	public Slice add(byte[] value) throws IOException {
//		System.out.println("add to log");
		//确保文件大小
		if (node.get() >= NODES) {
			ensureRoom();
		}
//		System.out.println(node.get() + ": " + Thread.currentThread().getName());

		//预先确定位置
		int pos = node.getAndIncrement();
		int index = pos * Util.SIZE_OF_VALUE;
		// 获取当前文件编号以及偏移地址
		Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
		SliceOutput output = addr.output();
		output.writeInt(currentFileNum.get());
		output.writeInt(index);
		// System.out.print("log fileNum = " + currentFileNum.get());
		// System.out.println(" fileOffset = " + (int)offset);
		
		// 分配mmap
		MappedByteBuffer data = fileChannel.map(MapMode.READ_WRITE, index, Util.SIZE_OF_VALUE);

		// value写入mmap
		data.put(value); //TODO 报错
		// 释放mmap
		Util.ByteBufferSupport.unmap(data);

		return addr;
	}

	public void ensureRoom() throws IOException {
		mutex.lock();
		try {
			//双重检查法
			if (node.get() >= NODES) {
				System.out.println("到达临界node = " + node.get());
				// 关闭channel
				Util.Closeables.closeQuietly(fileChannel);
				// 新建Log
				int newFileNum = this.currentFileNum.incrementAndGet();
				String newFileName = Util.Filename.logFileName(newFileNum);
				File log = new File(databaseDir, newFileName);
				if (!log.exists()) {
					log.createNewFile();
				}
				// 更新fileChannel和offset
				this.fileChannel = new RandomAccessFile(log, "rw").getChannel();
				this.node.set(0);
			}
		} finally {
			mutex.unlock();
		}
	}

	// 正确关闭
	public synchronized void close() throws IOException {
		Util.Closeables.closeQuietly(fileChannel);
	}
}
