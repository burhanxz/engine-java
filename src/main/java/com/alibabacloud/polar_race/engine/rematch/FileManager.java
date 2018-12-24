package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibabacloud.polar_race.engine.base.Util;

public class FileManager {
	private static final int BASE_NUM = 64;
	private static final Map<Integer, List<Integer>> fileList = new HashMap<>();
	
	private static int logNum = 0;
	
	//待测试
	public static void init(File databaseDir) {
		String[] fileNames = databaseDir.list();
		Arrays.sort(fileNames);
		// 遍历所有文件
		for (int i = 0; i < fileNames.length; i++) {
			String fileName = fileNames[i];
			// 添加未记录在manager中的log文件
			if (fileName.endsWith("log")) {
//				System.out.println("fileName = " + fileName);
				int fileNum = Integer.valueOf(fileName.split("\\.")[0]);
				int num = fileNum % BASE_NUM;
				List<Integer> list = null;
				if((list = fileList.get(num)) == null) {
					list = new ArrayList<>();
					fileList.put(num, list);
				}
				list.add(fileNum);
				logNum++;
			}
		}
		System.out.println("fileList: ");
		fileList.forEach((k,v) -> {
			System.out.print(k + " : ");
			v.forEach(e -> {
				System.out.print(e + ", ");
			});
			System.out.println();
		});
	}
	
	public static int getRealFileNum(int fileNum) {
		List<Integer> list = fileList.get(fileNum % BASE_NUM);
		if(list == null)
			return fileNum % BASE_NUM;
		return list.get(list.size() - 1);
	}
	
	public static Map<Integer, List<Integer>> getMap(){
		return fileList;
	}
	
	public static int getLogNum() {
		return logNum;
	}
}

