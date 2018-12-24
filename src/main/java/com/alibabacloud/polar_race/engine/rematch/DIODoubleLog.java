package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.alibabacloud.polar_race.engine.base.Util;

import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public class DIODoubleLog {
	private static DirectIoLib lib = DirectIoLib.getLibForPath(System.getProperty("java.io.tmpdir"));
	private final DirectIoByteChannel fileChannel;
	private final AlignedDirectByteBuffer buffer;
	
	private FileChannel fileChannel4k;
	private MappedByteBuffer data4k;

	private long offset4k;
	private final static int KEY_PAGE_SIZE = 1 << 19; // 1MB
	public DIODoubleLog(File databaseDir, int fileNum) throws Exception {
		// 获取文件名
		String fileName = Util.Filename.logFileName(fileNum);
		// 获取文件
		File log = new File(databaseDir, fileName);
		if (!log.exists())
			log.createNewFile();
		//获取draf
		fileChannel = DirectIoByteChannel.getChannel(log, false);
		//新建buffer
		buffer = AlignedDirectByteBuffer.allocate(lib, Util.SIZE_OF_VALUE);
		
		// 获取文件名
		String fileName4k = Util.Filename.keyLogFileName(fileNum);
		// 获取文件
		File log4k = new File(databaseDir, fileName4k);
		if (!log4k.exists())
			log4k.createNewFile();
		RandomAccessFile raf = new RandomAccessFile(log4k, "rw");
		fileChannel4k = raf.getChannel();
		offset4k = fileChannel.size() / Util.SIZE_OF_VALUE * 8;
		data4k = fileChannel4k.map(MapMode.READ_WRITE, offset4k, KEY_PAGE_SIZE);
	}
	
	public synchronized void add(byte[] key, byte[] value) throws Exception {
		buffer.clear();
		buffer.put(value);
		buffer.flip();
		fileChannel.write(buffer, fileChannel.size());
		
		ensureRoom();
		data4k.put(key);
	}
	
	private void ensureRoom() throws Exception {
		if (data4k.position() == KEY_PAGE_SIZE) {
			Util.ByteBufferSupport.unmap(data4k);
			offset4k += KEY_PAGE_SIZE;
			data4k = fileChannel4k.map(MapMode.READ_WRITE, offset4k, KEY_PAGE_SIZE);
		}
	}
	
	public void close() throws Exception {
		fileChannel.close();
		fileChannel4k.close();
	}
}
