package com.alibabacloud.polar_race.engine.common;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import com.alibabacloud.polar_race.engine.preliminary.PreDB;
import com.alibabacloud.polar_race.engine.preliminary.PreMemDB;
import com.alibabacloud.polar_race.engine.rematch.DB;
import com.alibabacloud.polar_race.engine.rematch.GroupDB;

public class EngineRace extends AbstractEngine {
//	private PreDB db;
//	private PreMemDB db;
	private GroupDB db;
//	private DB db;
	
	@Override
	public void open(String path) throws EngineException {
		try {
//			db = new PreMemDB(path);
			db = new GroupDB(path);
		} catch (Exception e) {
			e.printStackTrace();
			throw new EngineException(RetCodeEnum.IO_ERROR, "open");
		} 
	}
	
	@Override
	public void write(byte[] key, byte[] value) throws EngineException {
		try {
			db.write(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			throw new EngineException(RetCodeEnum.CORRUPTION, "write");
		}
	}
	
	@Override
	public byte[] read(byte[] key) throws EngineException {
		byte[] value = null;
		try {
			value = db.read(key);
//			if(Util.checkAllZero(new Slice(value))) {
//				System.out.println("value 全0 key = " + Util.bytesToLong(key));
//			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new EngineException(RetCodeEnum.NOT_FOUND, Util.getLittleEndianLong(key) + " read error " + e.getMessage());
		}
		if(value == null) {
			throw new EngineException(RetCodeEnum.NOT_FOUND, "read null");
		}
		return value;
	}
	
	@Override
	public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
		try {
			db.range(lower, upper, visitor);
		} catch (Exception e) {
			e.printStackTrace();
			throw new EngineException(RetCodeEnum.CORRUPTION, "range");
		}
		
	
	}
	
	@Override
	public void close() {
		try {
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
