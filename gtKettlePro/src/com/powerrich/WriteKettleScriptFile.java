package com.powerrich;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

public class WriteKettleScriptFile {

	private static final String TRANSFER_DELETE_TEMP = "TRANSFER_TEMP_DELETE.ktr";
	private static final String TRANSFER_UPDATE_TEMP = "TRANSFER_TEMP_UPDATE.ktr";
	private static final String TRANSFER_MODIFY_BASE_TEMP = "TRANSFER_TEMP_UPDATE_BASE.ktr";
	private static final String TABLE = "TABLE.csv";
	private static final String JOB_TEMP = "TRANSFER_JOB_TEMP.kjb";

	public static void main(String[] args) {
		Util kettleUtil = new Util();

		Connection con = kettleUtil.getConnection();

		String tables = readTemplateXML(kettleUtil.kettleTemplateDir + TABLE);
		System.out.println(tables);
		String[] tableArray = tables.split(",");
		System.out.println("长度："+tableArray.length);

		List<TransfEntity> list = null;
		try {
			KettleEnvironment.init();
			//Encr encr = new Encr();
			for (int i = 0; i < tableArray.length; i++) {
				String[] applyment = tableArray[i].split(";");
				System.out.println("循环"+i+"次，表名："+applyment[0]+",订阅放数量："+applyment.length);
				if(applyment.length==1) {
					list = kettleUtil.getTransfEntity(con, applyment[0], "");
				}else {
					StringBuffer inSql = new StringBuffer();
					for (int k = 1; k < applyment.length; k++) {
						System.out.println("订阅方式site no："+applyment[k]);
						inSql.append("'");
						inSql.append(applyment[k]);
						inSql.append("'");
						if(k!=applyment.length-1) {
							inSql.append(",");
						}
					}
					list = kettleUtil.getTransfEntity(con, applyment[0], inSql.toString());
				}
				System.out.println("查询到底长度：" + list.size());
				for (int j = 0; (list != null) && (list.size() != 0) && (j < list.size()); j++) {
					TransfEntity teEntity = (TransfEntity) list.get(j);
					System.out.println("cata_id" + teEntity.getCATA_ID());
					System.out.println("模板路径：" + kettleUtil.kettleTemplateDir + TRANSFER_DELETE_TEMP);

					/* 生成 删除 delete ktr 文件 */
					String trDelTempXML = readTemplateXML(kettleUtil.kettleTemplateDir + TRANSFER_DELETE_TEMP);
					String ktrDelContext = generateKtrContext(1, trDelTempXML, teEntity);
					System.out.println(kettleUtil.kettleJobDir + teEntity.getTRANSFER_DELETE_NAME() + ".ktr");
					writeKtrFile(kettleUtil.kettleJobDir + teEntity.getTRANSFER_DELETE_NAME() + ".ktr", ktrDelContext);

					/* 生成 同步数据 update ktr 文件 */
					String trUpdateTempXML = readTemplateXML(kettleUtil.kettleTemplateDir + TRANSFER_UPDATE_TEMP);
					String ktrUpdateContext = generateKtrContext(2, trUpdateTempXML, teEntity);
					System.out.println(kettleUtil.kettleJobDir + teEntity.getTRANSFER_UPDATE_NAME() + ".ktr");
					writeKtrFile(kettleUtil.kettleJobDir + teEntity.getTRANSFER_UPDATE_NAME() + ".ktr",
							ktrUpdateContext);

					/* 生成 更新基线 update base ktr 文件 */
					String trUpdateBaseTempXML = readTemplateXML(
							kettleUtil.kettleTemplateDir + TRANSFER_MODIFY_BASE_TEMP);
					String ktrUpdateBaseContext = generateKtrContext(3, trUpdateBaseTempXML, teEntity);
					System.out.println(kettleUtil.kettleJobDir + teEntity.getTRANSFER_UPDATE_BASE_NAME() + ".ktr");
					writeKtrFile(kettleUtil.kettleJobDir + teEntity.getTRANSFER_UPDATE_BASE_NAME() + ".ktr",
							ktrUpdateBaseContext);

					JobEntity jobEntity = kettleUtil.getJob(kettleUtil.jobInterval);
					jobEntity.setJOB_NAME(teEntity.getTRANSFER_PROVIDER_TABLENAME_APPLY() + "_JOB");
					jobEntity
							.setTRANSFER_JOB_DEL_STEP_NAME(teEntity.getTRANSFER_PROVIDER_TABLENAME_APPLY() + "_DELETE");
					jobEntity.setTRANSFER_JOB_DEL_STEP_FILE_PATH(
							kettleUtil.kettleJobDir + teEntity.getTRANSFER_DELETE_NAME() + ".ktr");
					jobEntity.setTRANSFER_JOB_SYNC_DATA_STEP_NAME(
							teEntity.getTRANSFER_PROVIDER_TABLENAME_APPLY() + "_UPDATE");
					jobEntity.setTRANSFER_JOB_SYNC_DATA_STEP_FILE_PATH(
							kettleUtil.kettleJobDir + teEntity.getTRANSFER_UPDATE_NAME() + ".ktr");
					jobEntity.setTRANSFER_JOB_UPDATE_BASE_STEP_NAME(
							teEntity.getTRANSFER_PROVIDER_TABLENAME_APPLY() + "_MODIFY_BASE");
					jobEntity.setTRANSFER_JOB_UPDATE_BASE_STEP_FILE_PATH(
							kettleUtil.kettleJobDir + teEntity.getTRANSFER_UPDATE_BASE_NAME() + ".ktr");

					String jobTempXML = readTemplateXML(kettleUtil.kettleTemplateDir + JOB_TEMP);
					String kjbContext = generateKJBContext(jobTempXML, jobEntity);
					System.out.println(kettleUtil.kettleJobDir + jobEntity.getJOB_NAME() + ".kjb");
					writeKtrFile(kettleUtil.kettleJobDir + jobEntity.getJOB_NAME() + ".kjb", kjbContext);
				}
			}
			kettleUtil.closeConnect(con);
			/*try {
				KettleEnvironment.init();
				List<String> jobFileList = getList(kettleUtil.kettleJobDir);
				for (int i = 0; jobFileList != null && jobFileList.size() != 0 && i < jobFileList.size(); i++) {
					JobMeta jm = new JobMeta((String) jobFileList.get(i), null);
					Job job = new Job(null, jm);
					job.start();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String generateKJBContext(String kjbTempXML, JobEntity je) {
		kjbTempXML = kjbTempXML.replaceAll("JOB_NAME", je.getJOB_NAME());
		kjbTempXML = kjbTempXML.replaceAll("TRANSFER_JOB_DEL_STEP_NAME", je.getTRANSFER_JOB_DEL_STEP_NAME());
		kjbTempXML = kjbTempXML.replaceAll("TRANSFER_JOB_DEL_STEP_FILE_PATH", je.getTRANSFER_JOB_DEL_STEP_FILE_PATH());
		kjbTempXML = kjbTempXML.replaceAll("TRANSFER_JOB_SYNC_DATA_STEP_NAME",
				je.getTRANSFER_JOB_SYNC_DATA_STEP_NAME());
		kjbTempXML = kjbTempXML.replaceAll("TRANSFER_JOB_SYNC_DATA_STEP_FILE_PATH",
				je.getTRANSFER_JOB_SYNC_DATA_STEP_FILE_PATH());
		kjbTempXML = kjbTempXML.replaceAll("TRANSFER_JOB_UPDATE_BASE_STEP_NAME",
				je.getTRANSFER_JOB_UPDATE_BASE_STEP_NAME());
		kjbTempXML = kjbTempXML.replaceAll("TRANSFER_JOB_UPDATE_BASE_STEP_FILE_PATH",
				je.getTRANSFER_JOB_UPDATE_BASE_STEP_FILE_PATH());
		kjbTempXML = kjbTempXML.replaceAll("JOB_INTERVAL_SECONDS", je.getJOB_INTERVAL_SECONDS());
		kjbTempXML = kjbTempXML.replaceAll("JOB_INTERVAL_MINUTES", je.getJOB_INTERVAL_MINUTES());
		/*替换日志数据库连接信息*/
	    kjbTempXML = kjbTempXML.replaceAll("LOG_DB_TABLE", je.getLOG_DB_TABLE());
	    kjbTempXML = kjbTempXML.replaceAll("LOG_DB_IP", je.getLOG_DB_IP());
	    kjbTempXML = kjbTempXML.replaceAll("LOG_DB_NAME", je.getLOG_DB_NAME());
	    kjbTempXML = kjbTempXML.replaceAll("LOG_DB_PORT", je.getLOG_DB_PORT());
	    kjbTempXML = kjbTempXML.replaceAll("LOG_DB_TYPE", je.getLOG_DB_TYPE());
	    kjbTempXML = kjbTempXML.replaceAll("LOG_DB_USERNAME", je.getLOG_DB_USERNAME());
	    kjbTempXML = kjbTempXML.replaceAll("LOG_DB_ENCREPTED_PASSWORD", Encr.encryptPasswordIfNotUsingVariables(je.getLOG_DB_ENCREPTED_PASSWORD()));
		return kjbTempXML;
	}

	public static String generateKtrContext(int stepNo, String ktrTempXML, TransfEntity te) {
		try {
			if (1 == stepNo) {
				String delSql = "SELECT ins FROM " + te.getSOURCE_TABLE_NAME()
						+ "_del where seq> (SELECT cur_seq FROM tex_res_record WHERE res_id = '" + te.getCATA_ID()
						+ "' and sub_org_id='" + te.getAPPLY_SITE_NO() + "')";
				ktrTempXML = ktrTempXML.replaceAll("TRANSFER_NAME", te.getTRANSFER_DELETE_NAME());
				ktrTempXML = ktrTempXML.replaceAll("SQL_DEL_TABLE_INPUT", delSql);
				ktrTempXML = ktrTempXML.replaceAll("TARGET_TABLE_NAME", te.getTARGET_TABLE_NAME());
			}
			if (2 == stepNo) {
				String inputSql = "SELECT " + te.getSOURCE_TABLE_FIELDS_STR()
						+ " ins, seq, updatestatus, updatetime FROM " + te.getSOURCE_TABLE_NAME()
						+ " where seq>(SELECT cur_seq as seq FROM tex_res_record WHERE res_id = '" + te.getCATA_ID()
						+ "' and sub_org_id='" + te.getAPPLY_SITE_NO() + "')";
				ktrTempXML = ktrTempXML.replaceAll("TRANSFER_NAME", te.getTRANSFER_UPDATE_NAME());
				ktrTempXML = ktrTempXML.replaceAll("SQL_SOURCE_TABLE_INPUT", inputSql);
				ktrTempXML = ktrTempXML.replaceAll("TARGET_TABLE_NAME", te.getTARGET_TABLE_NAME());
				ktrTempXML = ktrTempXML.replaceAll("TARGET_TABLE_FIELDS_XML", te.getTARGET_TABLE_FIELDS_XML());
			}
			if (3 == stepNo) {
				String queryMaxSeqSql = "SELECT '" + te.getCATA_ID() + "' res_id, '" + te.getAPPLY_SITE_NO()
						+ "' sub_org_id, CASE WHEN max(seq) is NULL THEN 0 ELSE MAX(seq) END AS cur_seq FROM "
						+ te.getTARGET_TABLE_NAME();
				ktrTempXML = ktrTempXML.replaceAll("TRANSFER_NAME", te.getTRANSFER_UPDATE_BASE_NAME());
				ktrTempXML = ktrTempXML.replaceAll("SQL_QUERY_SUBTABLE_MAXSEQ", queryMaxSeqSql);
			}
			/* 替换源数据库连接信息 */
			ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_IP", te.getSOURCE_DB_IP());
			ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_NAME", te.getSOURCE_DB_NAME());
			ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_PORT", te.getSOURCE_DB_PORT());
			ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_SCHEMA", te.getSOURCE_DB_SCHEMA());
			ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_TYPE", te.getSOURCE_DB_TYPE());
			ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_USERNAME", te.getSOURCE_DB_USERNAME());
			ktrTempXML = ktrTempXML.replaceAll("SOURCE_DATABASE_ENCREPTED_PASSWORD", Encr
					.encryptPasswordIfNotUsingVariables(Password.pWORD(te.getSOURCE_DATABASE_ENCREPTED_PASSWORD(), 2)));
			/* 替换目标数据库连接信息 */
			ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_IP", te.getTARGET_DB_IP());
			ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_NAME", te.getTARGET_DB_NAME());
			ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_PORT", te.getTARGET_DB_PORT());
			ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_SCHEMA", te.getTARGET_DB_SCHEMA());
			ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_TYPE", te.getTARGET_DB_TYPE());
			ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_USERNAME", te.getTARGET_DB_USERNAME());
			ktrTempXML = ktrTempXML.replaceAll("TARGET_DATABASE_ENCREPTED_PASSWORD", Encr
					.encryptPasswordIfNotUsingVariables(Password.pWORD(te.getTARGET_DATABASE_ENCREPTED_PASSWORD(), 2)));
			/*替换日志数据库连接信息*/
	        ktrTempXML = ktrTempXML.replaceAll("LOG_ENTRY_TABLE_NAME", te.getLOG_ENTRY_TABLE_NAME());
	        ktrTempXML = ktrTempXML.replaceAll("LOG_DB_IP", te.getLOG_DB_IP());
	        ktrTempXML = ktrTempXML.replaceAll("LOG_DB_NAME", te.getLOG_DB_NAME());
	        ktrTempXML = ktrTempXML.replaceAll("LOG_DB_PORT", te.getLOG_DB_PORT());
	        ktrTempXML = ktrTempXML.replaceAll("LOG_DB_TYPE", te.getLOG_DB_TYPE());
	        ktrTempXML = ktrTempXML.replaceAll("LOG_DB_USERNAME", te.getLOG_DB_USERNAME());
	        ktrTempXML = ktrTempXML.replaceAll("LOG_DB_ENCREPTED_PASSWORD", Encr.encryptPasswordIfNotUsingVariables(te.getLOG_DB_ENCREPTED_PASSWORD()));
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return ktrTempXML;
	}

	public static String readTemplateXML(String pathname) {
		String context = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(pathname)));
			String readLine = br.readLine();
			while (readLine != null) {
				context += readLine;
				readLine = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return context;
	}

	public static void writeKtrFile(String pathname, String ktrContent) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(new File(pathname)));
			bw.write(ktrContent);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeIO(bw);
		}
	}

	public static void closeIO(Object obj) {
		try {
			if (((obj instanceof BufferedReader)) && (obj != null)) {
				((BufferedReader) obj).close();
			} else if (((obj instanceof BufferedWriter)) && (obj != null)) {
				((BufferedWriter) obj).close();
			} else {
				System.out.println("没有这个类型");
			}
		} catch (IOException localIOException) {
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
				System.out.println(filePath);
				if (os.toUpperCase().startsWith("WIN")) {
					if (filePath.endsWith(".kjb")) {
						list.add(filePath);
					}
				} else {
					list.add(filePath);
				}
			}
		}
		return list;
	}
}
