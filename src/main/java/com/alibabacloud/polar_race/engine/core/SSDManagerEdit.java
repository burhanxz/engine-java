package com.alibabacloud.polar_race.engine.core;

import com.alibabacloud.polar_race.engine.base.FileMetaData;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;

public class SSDManagerEdit {
	// 数据已经完成compaction的log编号
	private final int compactedLogNum;
	// compaction产生的新文件 （24B）
	private final FileMetaData newFiles;
	// 下一个可用文件编号
	private final int nextFileNum;

	public SSDManagerEdit(Slice data) {
		this.compactedLogNum = data.getInt(0);
		this.newFiles = new FileMetaData(data.slice(Util.SIZE_OF_INT, Util.FILEMETADATA_SIZE));
		this.nextFileNum = data.getInt(Util.SIZE_OF_INT + Util.FILEMETADATA_SIZE);
	}

	public SSDManagerEdit(int compactedLogNum, FileMetaData newFiles, int nextFileNum) {
		this.compactedLogNum = compactedLogNum;
		this.newFiles = newFiles;
		this.nextFileNum = nextFileNum;
	}

	public int getCompactedLogNum() {
		return compactedLogNum;
	}

	public FileMetaData getNewFiles() {
		return newFiles;
	}

	public int getNextFileNum() {
		return nextFileNum;
	}
}
