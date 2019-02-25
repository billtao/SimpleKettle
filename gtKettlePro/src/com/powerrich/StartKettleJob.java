package com.powerrich;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.JobEntryLogTable;
import org.pentaho.di.core.logging.JobLogTable;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

public class StartKettleJob {

	public static void main(String[] args) {
		Util kettleUtil = new Util();
		try {
			KettleEnvironment.init();
			//Logger.getLogger("org.pentaho.di").addAppender(new FileAppender(new SimpleLayout(), kettleUtil.kettleTemplateDir+"logger.log"));
			List<String> jobFileList = getList(kettleUtil.kettleJobDir);
			for (int i = 0; jobFileList != null && jobFileList.size() != 0 && i < jobFileList.size(); i++) {
				JobMeta jm = new JobMeta((String) jobFileList.get(i), null);
				
				/*DatabaseMeta databaseMeta = new DatabaseMeta();
				databaseMeta.setName("1");
				databaseMeta.setDatabaseType(kettleUtil.logDbType);
				databaseMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
				databaseMeta.setHostname(kettleUtil.logDbIp);
				databaseMeta.setDBName(kettleUtil.logDbName);
				databaseMeta.setDBPort(kettleUtil.logDbPort);
				databaseMeta.setUsername(kettleUtil.logDbUsername);
				databaseMeta.setPassword(kettleUtil.logDbPassword);
				jm.addDatabase(databaseMeta);
				
				JobEntryLogTable jelt = JobEntryLogTable.getDefault(jm, jm);
				jelt.setConnectionName("1");
				jelt.setSchemaName(kettleUtil.logDbSchema);
				jelt.setTableName(kettleUtil.entryLogTable);
				jm.setJobEntryLogTable(jelt);
				
				JobLogTable jobLogTable = JobLogTable.getDefault(jm, jm);
				jobLogTable.setConnectionName("1");
				jobLogTable.setSchemaName(kettleUtil.logDbSchema);
				jobLogTable.setTableName(kettleUtil.jobLogTable);
				jm.setJobLogTable(jobLogTable);*/
				
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
