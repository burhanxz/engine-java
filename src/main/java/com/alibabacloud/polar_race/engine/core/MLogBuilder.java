package com.alibabacloud.polar_race.engine.core;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class MLogBuilder {
	private final FileChannel fileChannel;
	// private MappedByteBuffer mappedByteBuffer;
	private ByteBuffer byteBuffer;

	public MLogBuilder(File databaseDir) throws IOException {
		File manifest = new File(databaseDir, Util.MANIFEST_FILE_NAME);
		if (!manifest.exists())
			manifest.createNewFile();
		RandomAccessFile raf = new RandomAccessFile(manifest, "rw");
		raf.seek(raf.length()); //seek到文件末尾
		this.fileChannel = raf.getChannel();
		System.out.println("fileChannel.isOpen() = " + fileChannel.isOpen());
		this.byteBuffer = ByteBuffer.allocate(Util.SSDMANAGEREDIT_SIZE);
	}

	// 持久化SSDManagerEdit(32B)
	public void addEdit(SSDManagerEdit edit) throws IOException {
		// 根据edit建立持久化内容
		Slice slice = Slices.allocate(Util.SSDMANAGEREDIT_SIZE);
		SliceOutput sliceOutput = slice.output();
		// 添加vlog信息
		sliceOutput.writeInt(edit.getCompactedLogNum());
		// 添加filemetaData信息
		sliceOutput.writeInt(edit.getNewFiles().getNumber());
		sliceOutput.writeInt(edit.getNewFiles().getFileSize());
		sliceOutput.writeBytes(edit.getNewFiles().getSmallest());
		sliceOutput.writeBytes(edit.getNewFiles().getLargest());
		// 添加文件编号信息
		sliceOutput.writeInt(edit.getNextFileNum());

		/*
		 * 测试代码 assert !Util.checkAllZero(slice) : "logWriter写入全0数据";
		 */
		assert !Util.checkAllZero(slice) : "logWriter写入全0数据";

		byteBuffer.put(slice.getRawArray());
		byteBuffer.flip();
		assert fileChannel.isOpen() : "fileChannel 异常关闭";
		fileChannel.write(byteBuffer);
		byteBuffer.clear();
	}

	// 正确关闭
	public synchronized void close() throws IOException {

		Util.Closeables.closeQuietly(fileChannel);
	}

}
