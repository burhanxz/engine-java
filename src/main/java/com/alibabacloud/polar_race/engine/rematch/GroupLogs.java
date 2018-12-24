package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.preliminary.PreDoubleLog;

public class GroupLogs {
	private final File databaseDir;
	private final DoubleLog[] logs = new DoubleLog[Util.GROUPS];
	private final AtomicBoolean[] exists = new AtomicBoolean[Util.GROUPS];

	public GroupLogs(File databaseDir) throws Exception {
		this.databaseDir = databaseDir;
		for (int i = 0; i < Util.GROUPS; i++) {
			exists[i] = new AtomicBoolean(false);
		}
	}

	public void add(byte[] key, byte[] value) throws Exception {
		// 获取key高10位
		int fileNum = ((key[0] & 0xff) << 2) | ((key[1] & 0xff) >> 6);
		// 新建log
		if (!exists[fileNum].get()) {
			synchronized (exists[fileNum]) {
				if (!exists[fileNum].get()) {
					// 新建log
					logs[fileNum] = new DoubleLog(databaseDir, fileNum);
					exists[fileNum].set(true);
				}
			}
		}
		// 取log
		DoubleLog log = logs[fileNum];
		log.add(key, value);
		// PreDoubleLog log
	}
	
	public synchronized void close() throws Exception {
		for (int i = 0; i < Util.GROUPS; i++) {
			if(logs[i] != null) {
				logs[i].close();
			}
		}
	}
}
