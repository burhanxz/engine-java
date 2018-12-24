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

public class PreMMapLog implements PreLog{
	private static long LOG_FILE_SIZE = 1 << 30;
	private File databaseDir;
	private FileChannel fileChannel;
	private MappedByteBuffer data;
	private long offset;
	private AtomicInteger currentFileNum = new AtomicInteger(0);
	public PreMMapLog(File databaseDir, int fileNum) throws IOException {
		this.databaseDir = databaseDir;
		//获取文件名
		String fileName = Util.Filename.logFileName(fileNum);
		//获取文件
		File log = new File(databaseDir, fileName);
		if (!log.exists())
			log.createNewFile();
		
		RandomAccessFile raf = new RandomAccessFile(log, "rw");
		this.offset = raf.length();
		this.fileChannel = raf.getChannel();
		
		System.out.println("初始offset = " + offset);
		ensureRoom();
	}
	
	public synchronized Slice add(byte[] value) throws IOException {
//		System.out.println("add to log");
		//分配mmap
		data = fileChannel.map(MapMode.READ_WRITE, offset, Util.SIZE_OF_VALUE);
		
		//获取当前文件编号以及偏移地址
		Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
		SliceOutput output = addr.output();
		output.writeInt(currentFileNum.get());
		output.writeInt((int)offset);
//		System.out.print("log fileNum = " + currentFileNum.get());
//		System.out.println(" fileOffset = " + (int)offset);
		//value写入mmap
		data.put(value);
		
		//更新offset并回收mmap
		offset += data.position();
		Util.ByteBufferSupport.unmap(data);
		
		//确保空间
		ensureRoom();
		
		return addr;
	}
	
	public synchronized void ensureRoom() throws IOException {
		//超过1G
		if(this.offset >= LOG_FILE_SIZE) {
			System.out.println("到达临界offset = " + offset);
			//关闭channel
			Util.Closeables.closeQuietly(fileChannel);
			//新建Log
			int newFileNum = this.currentFileNum.incrementAndGet();
			String newFileName = Util.Filename.logFileName(newFileNum);
			File log = new File(databaseDir, newFileName);
			if(!log.exists()) {
				log.createNewFile();
			}
			//更新fileChannel和offset
			this.fileChannel = new RandomAccessFile(log, "rw").getChannel();
			this.offset = 0l;
		}

	}
	
	// 正确关闭
	public synchronized void close() throws IOException {
		Util.Closeables.closeQuietly(fileChannel);
	}
}
