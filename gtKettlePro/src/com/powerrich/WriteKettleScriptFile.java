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

	  private static final String TRANSFER_TEMP = "TRANSFER_TEMP.ktr";
	  private static final String TABLE = "TABLE.csv";
	  private static final String JOB_TEMP = "JOB_TEMP.kjb";
	  
	  public static void main(String[] args)
	  {
	    Util kettleUtil = new Util();
	    
	    Connection con = kettleUtil.getConnection();
	    
	    String tables = readTemplateXML(kettleUtil.kettleTemplateDir + TABLE);
	    String[] tableArray = tables.split(",");
	    
	    List<TransfEntity> list = null;
	    try
	    {
	      KettleEnvironment.init();
	      Encr encr = new Encr();
	      for (int i = 0; i < tableArray.length; i++)
	      {
	        String[] applyment = tableArray[i].split(";");
	        list = kettleUtil.getTransfEntity(con, applyment[0], applyment[1]);
	        System.out.println("查询到底长度："+list.size());
	        for (int j = 0; (list != null) && (list.size() != 0) && (j < list.size()); j++)
	        {
	          TransfEntity teEntity = (TransfEntity)list.get(j);
	          System.out.println("cata_id"+teEntity.getCATA_ID());
	          System.out.println("模板路径："+kettleUtil.kettleTemplateDir + TRANSFER_TEMP);
	          
	          String trTempXML = readTemplateXML(kettleUtil.kettleTemplateDir + TRANSFER_TEMP);
	          System.out.println("开始生成文件");
	          String ktrContext = generateKtrContext(encr, trTempXML, teEntity);
	          System.out.println(kettleUtil.kettleJobDir + teEntity.getTRANSFER_NAME() + ".ktr");
	          writeKtrFile(kettleUtil.kettleJobDir + teEntity.getTRANSFER_NAME() + ".ktr", ktrContext);
	          
	          JobEntity jobEntity = Util.getJob(kettleUtil.kettleJobDir, kettleUtil.jobInterval, teEntity.getTRANSFER_NAME());
	          String jobTempXML = readTemplateXML(kettleUtil.kettleTemplateDir + JOB_TEMP);
	          String kjbContext = generateKJBContext(jobTempXML, jobEntity);
	          System.out.println(kettleUtil.kettleJobDir + jobEntity.getJOB_NAME() + ".kjb");
	          writeKtrFile(kettleUtil.kettleJobDir + jobEntity.getJOB_NAME() + ".kjb", kjbContext);
	        }
	      }
	      kettleUtil.closeConnect(con);
	      try
	      {
	        //KettleEnvironment.init();
	        
	        List<String> list1 = getList(kettleUtil.kettleJobDir);
	        int i = 0;
	        do
	        {
	          JobMeta jm = new JobMeta((String)list1.get(i), null);
	          Job job = new Job(null, jm);
	          job.start();i++;
	          if ((list == null) || (list.size() == 0)) {
	            break;
	          }
	        } while (i < list.size());
	      }
	      catch (Exception e)
	      {
	        e.printStackTrace();
	      }
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
	    }
	  }
	  public static String generateKJBContext(String kjbTempXML, JobEntity je)
	  {
	    kjbTempXML = kjbTempXML.replaceAll("JOB_NAME", je.getJOB_NAME());
	    kjbTempXML = kjbTempXML.replaceAll("JOB_TRANSFER_NAME", je.getJOB_TRANSFER_NAME());
	    kjbTempXML = kjbTempXML.replaceAll("JOB_TRANSFER_PATH", je.getJOB_TRANSFER_PATH());
	    kjbTempXML = kjbTempXML.replaceAll("JOB_INTERVAL_SECONDS", je.getJOB_INTERVAL_SECONDS());
	    kjbTempXML = kjbTempXML.replaceAll("JOB_INTERVAL_MINUTES", je.getJOB_INTERVAL_MINUTES());
	    return kjbTempXML;
	  }
	  public static String generateKtrContext(Encr encr, String ktrTempXML, TransfEntity te)
	  {
	    String delSql = "SELECT ins FROM " + te.getSOURCE_TABLE_NAME() + "_del where seq> (SELECT cur_seq FROM tex_res_record WHERE res_id = '" + te.getCATA_ID() + "' and sub_org_id='" + te.getAPPLY_SITE_NO() + "')";
	    String inputSql = "SELECT " + te.getSOURCE_TABLE_FIELDS_STR() + " ins, seq, updatestatus, updatetime FROM " + te.getSOURCE_TABLE_NAME() + " where seq>(SELECT cur_seq as seq FROM tex_res_record WHERE res_id = '" + te.getCATA_ID() + "' and sub_org_id='" + te.getAPPLY_SITE_NO() + "')";
	    String queryMaxSeqSql = "SELECT '" + te.getCATA_ID() + "' res_id, '" + te.getAPPLY_SITE_NO() + "' sub_org_id, CASE WHEN max(seq) is NULL THEN 0 ELSE MAX(seq) END AS cur_seq FROM " + te.getTARGET_TABLE_NAME();
	    try
	    {
	      ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_IP", te.getSOURCE_DB_IP());
	      ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_NAME", te.getSOURCE_DB_NAME());
	      ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_PORT", te.getSOURCE_DB_PORT());
	      ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_SCHEMA", te.getSOURCE_DB_SCHEMA());
	      ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_TYPE", te.getSOURCE_DB_TYPE());
	      ktrTempXML = ktrTempXML.replaceAll("SOURCE_DB_USERNAME", te.getSOURCE_DB_USERNAME());
	      
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_IP", te.getTARGET_DB_IP());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_NAME", te.getTARGET_DB_NAME());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_PORT", te.getTARGET_DB_PORT());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_SCHEMA", te.getTARGET_DB_SCHEMA());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_TYPE", te.getTARGET_DB_TYPE());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_DB_USERNAME", te.getTARGET_DB_USERNAME());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_TABLE_FIELDS_XML", te.getTARGET_TABLE_FIELDS_XML());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_TABLE_NAME", te.getTARGET_TABLE_NAME());
	      ktrTempXML = ktrTempXML.replaceAll("TRANSFER_NAME", te.getTRANSFER_NAME());
	      ktrTempXML = ktrTempXML.replaceAll("TARGET_DATABASE_ENCREPTED_PASSWORD", Encr.encryptPasswordIfNotUsingVariables(Password.pWORD(te.getTARGET_DATABASE_ENCREPTED_PASSWORD(), 2)));
	      ktrTempXML = ktrTempXML.replaceAll("SOURCE_DATABASE_ENCREPTED_PASSWORD", Encr.encryptPasswordIfNotUsingVariables(Password.pWORD(te.getSOURCE_DATABASE_ENCREPTED_PASSWORD(), 2)));
	      ktrTempXML = ktrTempXML.replaceAll("SQL_DEL_TABLE_INPUT", delSql);
	      ktrTempXML = ktrTempXML.replaceAll("SQL_QUERY_SUBTABLE_MAXSEQ", queryMaxSeqSql);
	      ktrTempXML = ktrTempXML.replaceAll("SQL_SOURCE_TABLE_INPUT", inputSql);
	    }
	    catch (RuntimeException e)
	    {
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
			  while(readLine!=null) {
				  context += readLine;
				  readLine = br.readLine();
			  }
		  }catch(FileNotFoundException e) {
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
		  }catch(FileNotFoundException e) {
			  e.printStackTrace();
		  } catch (IOException e) {
			e.printStackTrace();
		  }finally {
			  closeIO(bw);
		  }
	  }
	  public static void closeIO(Object obj)
	  {
	    try
	    {
	      if (((obj instanceof BufferedReader)) && (obj != null)) {
	        ((BufferedReader)obj).close();
	      } else if (((obj instanceof BufferedWriter)) && (obj != null)) {
	        ((BufferedWriter)obj).close();
	      } else {
	        System.out.println("没有这个类型");
	      }
	    }
	    catch (IOException localIOException) {}
	  }
	  
	  public static List<String> getList(Object path)
	  {
	    File f = (path instanceof String) ? new File((String)path) : (File)path;
	    
	    String os = System.getProperty("os.name");
	    File[] flist = f.listFiles();
	    List<String> list = new ArrayList();
	    for (int i = 0; i < flist.length; i++) {
	      if (!flist[i].isDirectory())
	      {
	        String filePath = flist[i].getPath();
	        System.out.println(filePath);
	        if (os.toUpperCase().startsWith("WIN"))
	        {
	          if (filePath.endsWith(".kjb")) {
	            list.add(filePath);
	          }
	        }
	        else {
	          list.add(filePath);
	        }
	      }
	    }
	    return list;
	  }
}
