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

import net.smacke.jaydio.DirectRandomAccessFile;

public class SlicedDIOLogReader {
	private AtomicInteger threads = new AtomicInteger(0);
	private ThreadLocal<byte[]> bytes;

	private final DirectRandomAccessFile[][] drafs;

	public SlicedDIOLogReader(File databaseDir) throws Exception {
		this.bytes = new ThreadLocal<byte[]>() {
			@Override
			protected byte[] initialValue() {
				byte[] value = new byte[Util.SIZE_OF_VALUE];
				return value;
			}

		};
		Map<Integer, List<Integer>> map = FileManager.getMap();
		this.drafs = new DirectRandomAccessFile[64][];
		for (int i = 0; i < 64; i++) {
			// System.out.println("map.get(" + i + ").size() = " + map.get(i).size());
			drafs[i] = new DirectRandomAccessFile[map.get(i).size()];
		}

		for (Iterator<Integer> it = map.keySet().iterator(); it.hasNext();) {
			int i = it.next();
			List<Integer> list = map.get(i);
			int j = 0;
			for (Integer fileNum : list) {
				File logFile = new File(databaseDir, Util.Filename.logFileName(fileNum));
				if (!logFile.exists()) {
					System.out.println("读取时log文件不存在!");
					// return null;
				}
				drafs[i][j++] = new DirectRandomAccessFile(logFile, "r");
			}
		}

	}

	public byte[] getValue(final long addrVal) throws Exception {
		final int fileNum = (int) (addrVal >>> 48);
		final long fileOffset = addrVal & Util.ADDR_MASK;

		byte[] buffer = bytes.get();
		DirectRandomAccessFile draf = drafs[fileNum % 64][fileNum / 64];
		synchronized(draf) {
			draf.seek(fileOffset);
			draf.read(buffer);
		}
		return buffer;
	}

	public void close() throws Exception {
		for (int i = 0; i < Util.LOG_NUM; i++) {
			for (int j = 0; j < drafs[i].length; j++) {
				drafs[i][j].close();
			}
		}
	}

}
