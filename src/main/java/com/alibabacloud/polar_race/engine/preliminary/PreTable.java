package com.alibabacloud.polar_race.engine.preliminary;

import java.io.IOException;
import java.util.concurrent.Callable;

public interface PreTable {
	public void add(byte[] key, byte[] addr) throws IOException;
	public byte[] get(byte[] key) throws IOException;
	public Callable<?> closer();
}
