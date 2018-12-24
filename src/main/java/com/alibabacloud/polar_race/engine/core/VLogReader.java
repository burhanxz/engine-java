package com.alibabacloud.polar_race.engine.core;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.concurrent.Callable;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;

public class VLogReader {
	private FileChannel fileChannel;
	private MappedByteBuffer data;
	private long size;
	
	public VLogReader(FileChannel fileChannel) throws IOException {
		this.fileChannel = fileChannel;
		this.size = fileChannel.size();
		data = fileChannel.map(MapMode.READ_ONLY, 0, size);

	}
	
	public Slice getValue(int offset) {
		if(offset > this.size)
			return null;
		Slice value = Slices.copiedBuffer(data, offset + Util.SIZE_OF_KEY, Util.SIZE_OF_VALUE);
		return value;
	}
	
	public Slice getKey(int offset) {
		if(offset > this.size)
			return null;
		Slice key = Slices.copiedBuffer(data, offset, Util.SIZE_OF_KEY);
		return key;
	}
	
	public VLogIterator iterator() {
		return new VLogIterator();
	}
	
	public Callable<?> closer() {
		return new Closer(fileChannel, data);
	}

	private static class Closer implements Callable<Void> {
		// private final String name;
		private final Closeable closeable;
		private final MappedByteBuffer data;

		public Closer(Closeable closeable, MappedByteBuffer data) {
			// this.name = name;
			this.closeable = closeable;
			this.data = data;
		}

		public Void call() {
			Util.ByteBufferSupport.unmap(data);
			Util.Closeables.closeQuietly(closeable);
			return null;
		}
	}
	
	public class VLogIterator implements Iterator<Slice>{
		private int index = 0;
		
		@Override
		public boolean hasNext() {
			return index * Util.SIZE_OF_INPUT < size;
		}

		@Override
		//next key
		public Slice next() {
			int start = index * Util.SIZE_OF_INPUT;

			Slice ret = Slices.copiedBuffer(data, start, Util.SIZE_OF_KEY);
			
			++index;
			
			return ret;
		}
		
		public int offset() {
			return index * Util.SIZE_OF_INPUT;
		}
		
	}

}
