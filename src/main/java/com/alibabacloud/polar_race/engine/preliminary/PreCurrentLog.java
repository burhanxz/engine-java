package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class PreCurrentLog implements PreLog{
	private static long LOG_FILE_SIZE = 1 << 30;//实际用1G
//	private static long LOG_FILE_SIZE = 1 << 20;//测试用1M
	private File databaseDir;
	private FileChannel fileChannel;
	private MappedByteBuffer data;
	private long offset;
	private final int fileNum;
	public PreCurrentLog(File databaseDir, int fileNum) throws IOException {
		this.databaseDir = databaseDir;
		this.fileNum = fileNum;
		//获取文件名
		String fileName = Util.Filename.logFileName(fileNum);
		//获取文件
		File log = new File(databaseDir, fileName);
		if (!log.exists())
			log.createNewFile();
		
		RandomAccessFile raf = new RandomAccessFile(log, "rw");
		this.offset = raf.length();
		this.fileChannel = raf.getChannel();
		
		System.out.println("log编号 = " + fileNum + ", 初始offset = " + offset);
	}
	
	public synchronized Slice add(byte[] value) throws IOException {
		//分配mmap
		data = fileChannel.map(MapMode.READ_WRITE, offset, Util.SIZE_OF_VALUE);
		
		//获取当前文件编号以及偏移地址
		Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
		SliceOutput output = addr.output();
		output.writeInt(fileNum);
		output.writeInt((int)offset);
//		System.out.print("log fileNum = " + currentFileNum.get());
//		System.out.println(" fileOffset = " + (int)offset);
		//value写入mmap
		data.put(value);
		
		//更新offset并回收mmap
		offset += data.position();
		Util.ByteBufferSupport.unmap(data);

		return addr;
	}
	
	public synchronized boolean hasRoom() throws IOException {
		//超过规定大小
		if(this.offset >= LOG_FILE_SIZE) {
//			System.out.println("到达临界offset = " + offset);
			//关闭channel
			Util.Closeables.closeQuietly(fileChannel);
			return false;
		}
		return true;
	}
	
	// 正确关闭
	public synchronized void close() throws IOException {
		Util.Closeables.closeQuietly(fileChannel);
	}

	public long getOffset() {
		return offset;
	}

	public int getFileNum() {
		return fileNum;
	}
}
