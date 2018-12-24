package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.rematch.FileManager;
import com.google.common.cache.Cache;

public class SlicedLogReader {
	private AtomicInteger threads = new AtomicInteger(0);
	private ThreadLocal<ByteBuffer> buffers;
	
	private final FileChannel[][] channels;

	public SlicedLogReader(File databaseDir, int logCacheSize) throws Exception {
		this.buffers = new ThreadLocal<ByteBuffer>() {
			@Override
			protected ByteBuffer initialValue() {
				byte[] value = new byte[Util.SIZE_OF_VALUE];
				ByteBuffer buffer = ByteBuffer.wrap(value);
				return buffer;
			}

		};
		Map<Integer, List<Integer>> map = FileManager.getMap();
		this.channels = new FileChannel[64][];
		for(int i = 0; i < 64; i++) {
//			System.out.println("map.get(" + i + ").size() = " + map.get(i).size());
			channels[i] = new FileChannel[map.get(i).size()];
		}
		
		for(Iterator<Integer> it = map.keySet().iterator(); it.hasNext();) {
			int i = it.next();
			List<Integer> list = map.get(i);
			int j = 0;
			for(Integer fileNum : list) {
				File logFile = new File(databaseDir, Util.Filename.logFileName(fileNum));
				if (!logFile.exists()) {
					System.out.println("读取时log文件不存在!");
					// return null;
				}
				RandomAccessFile raf = new RandomAccessFile(logFile, "r");
				FileChannel channel = raf.getChannel();
				channels[i][j++] = channel;
			}
		}

	}

	public byte[] getValue(final long addrVal) throws Exception {
		final int fileNum = (int) (addrVal >>> 48);
		final long fileOffset = addrVal & Util.ADDR_MASK;

		ByteBuffer buffer = buffers.get();
		buffer.clear();
		channels[fileNum % 64][fileNum / 64].read(buffer, fileOffset);

		return buffer.array();
	}

	public byte[] getValue(Slice addr) throws Exception {
		throw new UnsupportedOperationException();
	}

	public FileChannel getChannel(int fileNum) throws Exception {
		return channels[fileNum % 64][fileNum / 64];
	}

	public void close() throws Exception {
		for (int i = 0; i < Util.LOG_NUM; i++) {
			for(int j = 0; j < channels[i].length; j++) {
				channels[i][j].close();
			}
		}
	}
	
}

