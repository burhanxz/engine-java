package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;

import com.alibabacloud.polar_race.engine.base.Util;

import net.smacke.jaydio.DirectRandomAccessFile;

public class GroupLogReader {
	private ThreadLocal<byte[]> bytes;
	private final DirectRandomAccessFile[] drafs = new DirectRandomAccessFile[Util.GROUPS];

	public GroupLogReader(File databaseDir) throws Exception {
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

	public byte[] getValue(final long addrVal) throws Exception {
		final int fileNum = (int) (addrVal >>> 48);
		final long fileOffset = addrVal & Util.ADDR_MASK;

		byte[] buffer = bytes.get();
		DirectRandomAccessFile draf = drafs[fileNum];
		synchronized (draf) {
			draf.seek(fileOffset);
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
