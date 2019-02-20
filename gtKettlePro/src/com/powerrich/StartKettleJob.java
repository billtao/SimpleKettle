package com.powerrich;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

public class StartKettleJob {

	public static void main(String[] args) {
		Util kettleUtil = new Util();
		try {
			KettleEnvironment.init();
			List<String> jobFileList = getList(kettleUtil.kettleJobDir);
			for (int i = 0; jobFileList != null && jobFileList.size() != 0 && i < jobFileList.size(); i++) {
				JobMeta jm = new JobMeta((String) jobFileList.get(i), null);
				Job job = new Job(null, jm);
				job.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static List<String> getList(Object path) {
		File f = (path instanceof String) ? new File((String) path) : (File) path;
		String os = System.getProperty("os.name");
		File[] flist = f.listFiles();
		List<String> list = new ArrayList();
		for (int i = 0; i < flist.length; i++) {
			if (!flist[i].isDirectory()) {
				String filePath = flist[i].getPath();
				if (os.toUpperCase().startsWith("WIN")) {
					if (filePath.endsWith(".kjb")) {
						System.out.println(filePath);
						list.add(filePath);
					}
				} else {
					if (filePath.endsWith(".kjb")) {
						System.out.println(filePath);
						list.add(filePath);
					}
				}
			}
		}
		return list;
	}
}
