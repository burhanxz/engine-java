package com.alibabacloud.polar_race.engine.preliminary;

import java.io.IOException;

import com.alibabacloud.polar_race.engine.base.Slice;

public interface PreLog {
	public Slice add(byte[] value) throws IOException;
	public void close() throws IOException;
}
