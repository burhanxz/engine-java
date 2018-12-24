package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;

import com.alibabacloud.polar_race.engine.base.Util;

import net.smacke.jaydio.DirectRandomAccessFile;

public class GroupBinaryLogReader {
	private ThreadLocal<byte[]> bytes;
	private final DirectRandomAccessFile[] drafs = new DirectRandomAccessFile[Util.GROUPS];

	public GroupBinaryLogReader(File databaseDir) throws Exception {
		this.bytes = new ThreadLocal<byte[]>() {
			@Override
			protected byte[] initialValue() {
				byte[] value = new byte[Util.SIZE_OF_VALUE];
				return value;
			}

		};

		for (int i = 0; i < Util.GROUPS; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (logFile.exists()) {
				drafs[i] = new DirectRandomAccessFile(logFile, "r");
			}
		}
	}

	public byte[] getValue(final int fileNum, final int addrVal) throws Exception {

		byte[] buffer = bytes.get();
		DirectRandomAccessFile draf = drafs[fileNum];
		synchronized (draf) {
			draf.seek(addrVal);
			draf.read(buffer);
		}
		return buffer;
	}
	
	public DirectRandomAccessFile getDraf(int fileNum) {
		return drafs[fileNum];
	}

	public void close() throws Exception {
		for (int i = 0; i < Util.GROUPS; i++) {
			if(drafs[i] != null) {
				drafs[i].close();
			}
		}
	}
}
