package com.alibabacloud.polar_race.engine.core;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class VLogBuilder {
	// private static final int PAGE_SIZE = 2 * (1 << 12); // 2 * 4KB 适应SSD
	private static final int DATA_SIZE = Util.SIZE_OF_KEY + Util.SIZE_OF_VALUE; // 数据长度
	// private static final int HEADER_SIZE = 6; //头部长度
	// private static final int SIZE_OF_MAGIC_NUMBER = (1 << 12) - 7 - 8; //魔数长度
	// private static final int SIZE_OF_MAGIC_NUMBER = (1 << 12) - 8; // 魔数长度
	// private static final byte[] MAGIC_NUMBER = new byte[SIZE_OF_MAGIC_NUMBER];
	// private static final Slice MAGIC_SLICE;
	// static {
	// // 生成魔数
	// new Random().nextBytes(MAGIC_NUMBER);
	// MAGIC_SLICE = new Slice(MAGIC_NUMBER);
	// }

	// private long tail = 0;
	// private long head = 0;
	private final File file;
	private final int fileNumber;
	private final FileChannel fileChannel;
	private final AtomicBoolean closed = new AtomicBoolean();
	private MappedByteBuffer mappedByteBuffer;
	private int fileOffset;

	public VLogBuilder(File file, int fileNumber) throws IOException {
		requireNonNull(file, "file is null");
		checkArgument(fileNumber >= 0, "fileNumber is negative");
		this.file = file;
		this.fileOffset = 0;
		this.fileNumber = fileNumber;
		this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
		mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, DATA_SIZE);

	}

	// 各线程互斥地写入日志
	public synchronized Slice addRecord(Slice key, Slice value) throws IOException {
		assert !closed.get() : "Log has been closed.";
		assert key.length() == Util.SIZE_OF_KEY : "input key error.";
		assert value.length() == Util.SIZE_OF_VALUE : "input value error.";
		// 生成addr数值以及slice
		Slice addr = Slices.allocate(Util.SIZE_OF_LONG);
		SliceOutput addrOut = addr.output();
		addrOut.writeInt(fileNumber);
		addrOut.writeInt(fileOffset);
//		log.debug("addrValue = " + addrValue);
		//写入key-value到mappedByteBuffer
		mappedByteBuffer.put(key.getRawArray());
		mappedByteBuffer.put(value.getRawArray());

		fileOffset += mappedByteBuffer.position();
		// TODO 存疑 重设mappedByteBuffer
		Util.ByteBufferSupport.unmap(mappedByteBuffer);
		mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset, DATA_SIZE);


		return addr;
	}

	public Slice get(long addr) {

		return null;
	}

	public void updateHead() {

	}

	public synchronized void close() throws IOException {
		//已关闭，无需再关闭
		if(closed.get())
			return;
		
		closed.set(true);

		destroyMappedByteBuffer();

		if (fileChannel.isOpen()) {
			fileChannel.truncate(fileOffset);
		}

		// close the channel
		Util.Closeables.closeQuietly(fileChannel);
	}

	private void destroyMappedByteBuffer() {
		if (mappedByteBuffer != null) {
			fileOffset += mappedByteBuffer.position();
			Util.ByteBufferSupport.unmap(mappedByteBuffer);
		}
		mappedByteBuffer = null;
	}
	// private void ensureCapacity(int cap) throws IOException {
	// if (mappedByteBuffer.remaining() < cap) {
	// // remap
	// fileOffset += mappedByteBuffer.position();
	// Util.ByteBufferSupport.unmap(mappedByteBuffer);
	// mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset,
	// PAGE_SIZE);
	// }
	// }
	//
	// public long getTail() {
	// return tail;
	// }
	//
	// public long getHead() {
	// return head;
	// }

	public File getFile() {
		return file;
	}

	public int getFileNumber() {
		return fileNumber;
	}

	public long getFileOffset() {
		return fileOffset;
	}
}
