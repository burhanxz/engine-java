package com.alibabacloud.polar_race.engine.core;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.concurrent.Callable;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceComparator;
import com.alibabacloud.polar_race.engine.base.Slices;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.bloomfilter.BloomFilter;
import com.alibabacloud.polar_race.engine.bloomfilter.BloomFilters;
import com.alibabacloud.polar_race.engine.bloomfilter.DynamicBloomFilter;

public class Table {
	private final MappedByteBuffer data;
	private final SliceComparator sliceComparator = new SliceComparator();
	private final int metaBlockOffset;
	private final FileChannel fileChannel;
	private final Slice metaBlock;
	private final int blockNum;
	private final int metaBlockSize;
	
	public Table(FileChannel fileChannel) throws IOException {
		this.fileChannel = fileChannel;
		long size = fileChannel.size();
		data = fileChannel.map(MapMode.READ_ONLY, 0, size);
		//最后4位的数字代表metablock起始位置
		Slice metaBlockOffsetSlice = Slices.copiedBuffer(data, (int) size - Util.SIZE_OF_INT, Util.SIZE_OF_INT);
		metaBlockOffset = metaBlockOffsetSlice.getInt(0);
		this.metaBlockSize = (int) size - Util.SIZE_OF_INT - metaBlockOffset;
		
		this.blockNum = (int) Math.ceil(metaBlockOffset / (double)Util.blockSize);
//		System.out.println("metaBlock位置: " + metaBlockOffset);
		this.metaBlock = Slices.copiedBuffer(data, metaBlockOffset, metaBlockSize);
		//		this.metaBlockSize = blockNum * Util.META_BLOCK_ENTRY_SIZE;
		System.out.println("metaBlock位置: " + metaBlockOffset);
		System.out.println("单位metaBlock大小: " + Util.META_BLOCK_ENTRY_SIZE);
		System.out.println("总metaBlock大小: " + metaBlockSize);
	}

	//TODO 利用bloomfilter
	public int getBlockLocation(Slice key) {
		//获取所在的读任务线程 所持有的DynamicBloomFilter
		BloomFilter filter = BloomFilters.getBloomFilter();
		int ret = 0;
		for(int i = 0; i < this.blockNum; i++) {
			//确定起始位置
			int start = i * Util.META_BLOCK_ENTRY_SIZE;
			int len = Util.META_BLOCK_ENTRY_SIZE;
			if(start + len > metaBlockSize) {
				len = metaBlockSize - start;
			}
			
			Slice result = metaBlock.slice(start, len);
			if(filter.keyMayMatch(key, result)) {
				//ret的32位每位代表相应的block是否存在key
				ret |= (1 << i);
			}
		}

		return ret;
	}
	
	public TableIterator iterator() {
		return new TableIterator();
	}

	public class TableIterator implements Iterator<Slice>, Comparable<TableIterator> {
		// private Entry<KeyAndSeq, Slice> next = null;
		private int index;
		private int end;
		
		public TableIterator() {
			index = 0;
			end = -1;
		}
		
		public void reset() {
			index = 0; 
			end = -1;
		}
		
		public void setBlock(int blockNum) {
			index = blockNum * Util.SIZE_OF_BLOCK_ENTRY * Util.BLOCK_ENTRY_NUMBER;
			end = (blockNum + 1) * Util.SIZE_OF_BLOCK_ENTRY * Util.BLOCK_ENTRY_NUMBER;
			//如果end位置超过Meta  block起始位置，则重设
			if(end > metaBlockOffset) {
				end = metaBlockOffset;
			}
		}

		@Override
		public boolean hasNext() {
			if(end > 0) {
				return index < end;
			}
			return index < metaBlockOffset;
		}

		public Slice peek() {
			Slice slice = Slices.copiedBuffer(data, index, Util.SIZE_OF_BLOCK_ENTRY);
//			KeyAndSeq key = new KeyAndSeq(slice.slice(0, Util.SIZE_OF_KEYANDSEQ));
//			Slice addr = slice.slice(Util.SIZE_OF_KEYANDSEQ, Util.SIZE_OF_BLOCK_ENTRY);
//			return Maps.immutableEntry(key, addr);
			return slice;
		}

		@Override
		public Slice next() {
//			Entry<KeyAndSeq, Slice> ret = peek();
			Slice ret = peek();
			index += Util.SIZE_OF_BLOCK_ENTRY;
			return ret;
		}

		@Override
		public int compareTo(TableIterator that) {
			//对比peek()得到的slice所代表的keyAndSeq
			return sliceComparator.compare(this.peek().slice(0, Util.SIZE_OF_KEY), 
					that.peek().slice(0, Util.SIZE_OF_KEY));
		}

	}
	
    public Callable<?> closer()
    {
        return new Closer(/*name,*/ fileChannel, data);
    }

    private static class Closer
            implements Callable<Void>
    {
//        private final String name;
        private final Closeable closeable;
        private final MappedByteBuffer data;

        public Closer(/*String name, */Closeable closeable, MappedByteBuffer data)
        {
//            this.name = name;
            this.closeable = closeable;
            this.data = data;
        }

        public Void call()
        {
            Util.ByteBufferSupport.unmap(data);
            Util.Closeables.closeQuietly(closeable);
            return null;
        }
    }

	public int getMetaBlockOffset() {
		return metaBlockOffset;
	}


}
