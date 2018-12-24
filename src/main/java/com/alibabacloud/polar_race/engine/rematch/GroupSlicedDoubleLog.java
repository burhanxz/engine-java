package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Future;

import com.alibabacloud.polar_race.engine.base.Util;

public class GroupSlicedDoubleLog {
	private static long LOG_FILE_SIZE = Util.LOG_FILE_SIZE; // 文件最大128MB
	private final File databaseDir;
	private final int fileNum;
	private int realFileNum;
	private FileChannel fileChannel;
	private FileChannel fileChannel4k;
	private MappedByteBuffer data4k;
	private Future<MappedByteBuffer> newData4k;
	private long offset4k;
	private final static int KEY_PAGE_SIZE = 1 << 12; // 1MB
	public GroupSlicedDoubleLog(File databaseDir, int fileNum) throws Exception {
		this.databaseDir = databaseDir;
		this.fileNum = fileNum;
		//获取最新的log文件编号
		realFileNum = FileManager.getRealFileNum(fileNum);
		// 获取文件名
		String fileName = Util.Filename.logFileName(realFileNum);
		// 获取文件
		File log = new File(databaseDir, fileName);
		if (!log.exists())
			log.createNewFile();
		//获取draf
		fileChannel = new RandomAccessFile(log, "rw").getChannel();
		
		// 获取文件名
		String fileName4k = Util.Filename.keyLogFileName(fileNum);
		// 获取文件
		File log4k = new File(databaseDir, fileName4k);
		if (!log4k.exists())
			log4k.createNewFile();
		fileChannel4k = new RandomAccessFile(log4k, "rw").getChannel();
		//TODO 实际位置这里要改
		offset4k = Util.slicedRealNodes(databaseDir, fileNum);
		data4k = fileChannel4k.map(MapMode.READ_WRITE, offset4k, KEY_PAGE_SIZE);
	
		//如果文件过大，切分文件
		if (fileChannel.size() >= LOG_FILE_SIZE) {
			//截断文件并且关闭文件
			if (fileChannel.isOpen()) {
				fileChannel.close();
				// 获取新文件名
				this.realFileNum += Util.GROUPS;
				fileName = Util.Filename.logFileName(realFileNum);
				// 获取文件
				log = new File(databaseDir, fileName);
				if (!log.exists())
					log.createNewFile();
				// 获取channel
				fileChannel = new RandomAccessFile(log, "rw").getChannel();
			}
		}
	}
	
	public synchronized void add(byte[] key, byte[] value) throws Exception {
		ensureSize();
		fileChannel.write(ByteBuffer.wrap(value));
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
	
	private void ensureSize() throws Exception {
		//如果文件过大，切分文件
		if (fileChannel.size() >= LOG_FILE_SIZE) {
			//关闭文件
			if (fileChannel.isOpen()) {
				fileChannel.close();
				// 获取新文件名
				this.realFileNum += Util.GROUPS;
				String fileName = Util.Filename.logFileName(realFileNum);
				// 获取文件
				File log = new File(databaseDir, fileName);
				if (!log.exists())
					log.createNewFile();
				// 获取channel
				fileChannel = new RandomAccessFile(log, "rw").getChannel();
			}
		}
	}
	
	public void close() throws Exception {
		fileChannel.close();
		fileChannel4k.close();
	}
}
