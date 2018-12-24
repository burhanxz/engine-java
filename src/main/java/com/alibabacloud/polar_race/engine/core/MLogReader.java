package com.alibabacloud.polar_race.engine.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class MLogReader {
	private final FileChannel fileChannel;

	public MLogReader(FileChannel fileChannel) {
		this.fileChannel = fileChannel;

	}

	public Slice readEdit() throws IOException {
		Slice slice = Slices.allocate(Util.SSDMANAGEREDIT_SIZE);
		ByteBuffer buffer = ByteBuffer.wrap(slice.getRawArray());
		int bytes = fileChannel.read(buffer);
		if(Util.checkAllZero(slice) && bytes > 0) {
			System.out.println("logreader -readbytes: " + bytes);
			System.out.println("logreader -compactedLogNum: " + slice.getInt(0));
		}
		assert !(Util.checkAllZero(slice) && bytes > 0) : "logReader读出全0数据";

		/*
		 * 测试代码 assert !Util.checkAllZero(slice) && bytes > 0 : "logReader读出全0数据";
		 * System.out.println("logreader -readbytes: " + bytes);
		 * System.out.println("logreader -compactedLogNum: " + slice.getInt(0));
		 */
		if (bytes == -1)
			return null;
		return slice;
	}
}
