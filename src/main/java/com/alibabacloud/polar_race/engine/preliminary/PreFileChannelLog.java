package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.core.SSDManagerEdit;

public class PreFileChannelLog implements PreLog{
	private FileChannel fileChannel;
	// private MappedByteBuffer mappedByteBuffer;
	private ByteBuffer byteBuffer;
	private AtomicInteger currentFileNum = new AtomicInteger(0);
	private long fileOffset;
	private RandomAccessFile raf;
	private final File databaseDir;
	//fileNum总是获取磁盘中最大的
	public PreFileChannelLog(File databaseDir, int fileNum) throws IOException {
		this.databaseDir = databaseDir;
		//获取文件名
		String fileName = Util.Filename.logFileName(fileNum);
		//获取文件
		File log = new File(databaseDir, fileName);
		if (!log.exists())
			log.createNewFile();
		this.raf = new RandomAccessFile(log, "rw");
		//更新文件偏移位置
		this.fileOffset = raf.length();
		raf.seek(fileOffset);//seek到文件末尾
		this.fileChannel = raf.getChannel();
		System.out.println("fileChannel.isOpen() = " + fileChannel.isOpen());
		this.byteBuffer = ByteBuffer.allocate(Util.SIZE_OF_VALUE);
		
		ensureRoom();
	}

	// 持久化SSDManagerEdit(32B)
	public synchronized Slice add(byte[] value) throws IOException {
		//获取当前文件编号以及偏移地址
		Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
		SliceOutput output = addr.output();
		output.write(currentFileNum.get());
		output.write((int)fileOffset);
		
		//写入文件
		byteBuffer.put(value);
		byteBuffer.flip();
		fileChannel.write(byteBuffer);
		byteBuffer.clear();
		
		//更新偏移
		this.fileOffset += Util.SIZE_OF_VALUE;
		//确认空间
		ensureRoom();
		return addr;
	}
	
	public synchronized void ensureRoom() throws IOException {
		if(fileOffset > (1 << 30)) {
			Util.Closeables.closeQuietly(fileChannel);
			Util.Closeables.closeQuietly(raf);
			
			int newFileNum = this.currentFileNum.incrementAndGet();
			String newFileName = Util.Filename.logFileName(newFileNum);
			File log = new File(databaseDir, newFileName);
			if(!log.exists()) {
				log.createNewFile();
			}
			this.raf = new RandomAccessFile(log, "rw");
			this.fileChannel = raf.getChannel();
			this.fileOffset = 0;
		}
	}

	// 正确关闭
	public synchronized void close() throws IOException {

		Util.Closeables.closeQuietly(fileChannel);
		Util.Closeables.closeQuietly(raf);
	}

}
