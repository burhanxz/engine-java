package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Future;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.preliminary.Works;
@Deprecated
public class RangeValueLog {
	private static int NEW_DATA_POS = Util.VALUE_PAGE / 2;

	private FileChannel fileChannel;
	private MappedByteBuffer data;
	private Future<MappedByteBuffer> newData;
	private long offset;


	public RangeValueLog(File databaseDir, int fileNum) throws Exception {
		// 获取文件名
		String fileName = Util.Filename.logFileName(fileNum);

		// 获取文件
		File log = new File(databaseDir, fileName);

		if (!log.exists())
			log.createNewFile();

		RandomAccessFile raf = new RandomAccessFile(log, "rw");
		this.fileChannel = raf.getChannel();
		this.offset = Util.realSize(fileChannel, Util.VALUE_PAGE);
		this.data = fileChannel.map(MapMode.READ_WRITE, offset, Util.VALUE_PAGE);
	}


	public void add(byte[] value) throws Exception {
		ensureRoom();
		data.put(value);
	}

	public synchronized void ensureRoom() throws Exception {
		if (data.position() == Util.VALUE_PAGE) {
			// Util.ByteBufferSupport.unmap(data);
			Works.unmap(data);
			offset += Util.VALUE_PAGE;
			// TODO 异步分配mmap
			// data = fileChannel.map(MapMode.READ_WRITE, offset, Util.VALUE_PAGE);
			data = newData.get();
		} else if (data.position() == NEW_DATA_POS) {
			long newOffset = offset + Util.VALUE_PAGE;
			newData = Works.map(fileChannel, newOffset, Util.VALUE_PAGE);
		}
	}


	// 正确关闭
	public synchronized void close() throws IOException {
		if (fileChannel != null && !fileChannel.isOpen()) {
			fileChannel.close();
		}
		// Util.Closeables.closeQuietly(fileChannel);
	}

}

