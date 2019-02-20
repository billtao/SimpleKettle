package com.powerrich;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;

public class Util {

	public String driver;
	public String url;
	public String username;
	public String password;
	public Connection connection;
	public String kettleTemplateDir;
	public String kettleJobDir;
	public String jobInterval;
	
	public Util()
	{
	    Properties prop = new Properties();
	    try
	    {
	      prop.load(getClass().getResourceAsStream("config.properties"));
	      this.driver = prop.getProperty("driver");
	      this.url = prop.getProperty("url");
	      this.username = prop.getProperty("username");
	      this.password = prop.getProperty("password");
	      this.kettleTemplateDir = prop.getProperty("kettleTemplateDir");
	      this.kettleJobDir = prop.getProperty("kettleJobDir");
	      this.jobInterval = prop.getProperty("jobInterval", "10");
	    }
	    catch (IOException e)
	    {
	      System.out.println("读取配置文件config.properties文件出错！");
	      e.printStackTrace();
	    }
	}
	 public Connection getConnection()
	 {
	    try
	    {
	      Class.forName(this.driver);
	      this.connection = DriverManager.getConnection(this.url, this.username, this.password);
	    }
	    catch (ClassNotFoundException e)
	    {
	      System.out.println("加载数据库驱动出错！");
	      e.printStackTrace();
	    }
	    catch (SQLException e)
	    {
	      System.out.println("连接数据库出错！");
	      e.printStackTrace();
	    }
	    return this.connection;
	  }
	 
	  public static JobEntity getJob(String inteval)
	  {
	    JobEntity je = new JobEntity();
	    je.setJOB_INTERVAL_MINUTES(inteval);
	    return je;
	  }
	  
	  public List<TransfEntity> getTransfEntity(Connection con, String tablename, String applySiteNo){
		  String sql = "select n.DB_IP source_db_ip,n.DB_USERNAME source_db_username,n.DB_PASSWORD source_db_password, "
				  +" n.DB_TYPE source_db_type,n.DB_SID source_db_sid,n.DB_PORT source_db_port,"
				  + " s.apply_site_no,s.apply_site_name,s.prov_dept_id,s.prov_dept_name,t.*,"
				  + " s.table_eng_name,s.query_by,a.db_no,a.db_url,a.db_user,a.db_pass,a.db_type "
				  + " from TRES_SRV_SUBS_APPLY_INDEX t,tex_subs_subs s,TRES_SRV_SUBS_APPLY a,tmon_deploy_nodes n "
				  + " where t.srv_subs_id = s.srv_subs_id "
				  + " and t.cata_id = s.cata_id "
				  + " and t.srv_subs_id=a.srv_subs_id "
				  + " and n.org_code = s.PROV_DEPT_ID "
				  + " and t.is_pass=1 "
				  + " and s.state = 0 "
				  + " and s.realstate = 1 "
				  + " and s.table_eng_name='"+tablename+"'";
		  if(applySiteNo!=null||!"".equals(applySiteNo)) {
			  sql = sql + " and s.apply_site_no='"+applySiteNo+"'";
		  }
		  sql = sql+ " order by s.apply_site_no ";
		  System.out.println(sql);
		  List list = new ArrayList<TransfEntity>();
		  StringBuffer ttfx = null;
		  StringBuffer stfs = null;
		  Statement stmt = null;
		  ResultSet rs = null;
		  String seq = "";
		  try {
			  stmt = con.createStatement();
			  rs = stmt.executeQuery(sql);
			  int i = 0;
			  String tempSiteNo = "";
			  TransfEntity te = null;
			  while(rs.next()) {
				  if(!tempSiteNo.equals(rs.getString("APPLY_SITE_NO"))) {
					  seq = getSerialNo();
					  ttfx = new StringBuffer();
					  stfs = new StringBuffer();
					  te = new TransfEntity();
					  i = 0;
					  tempSiteNo = rs.getString("APPLY_SITE_NO");
					  list.add(te);
				  }
				  if(i==0) {
					  te.setSOURCE_TABLE_NAME(rs.getString("TABLE_ENG_NAME"));
					  te.setTARGET_TABLE_NAME(rs.getString("TABLE_ENG_NAME"));
					  te.setTRANSFER_DELETE_NAME(rs.getString("PROV_DEPT_ID")+"_"+rs.getString("TABLE_ENG_NAME")+"_"+rs.getString("APPLY_SITE_NO")+"_DELETE_"+seq);
					  te.setTRANSFER_UPDATE_NAME(rs.getString("PROV_DEPT_ID")+"_"+rs.getString("TABLE_ENG_NAME")+"_"+rs.getString("APPLY_SITE_NO")+"_UPDATE_"+seq);
					  te.setTRANSFER_UPDATE_BASE_NAME(rs.getString("PROV_DEPT_ID")+"_"+rs.getString("TABLE_ENG_NAME")+"_"+rs.getString("APPLY_SITE_NO")+"_UPDATE_BASE_"+seq);
					  te.setTRANSFER_PROVIDER_TABLENAME_APPLY(rs.getString("PROV_DEPT_ID")+"_"+rs.getString("TABLE_ENG_NAME")+"_"+rs.getString("APPLY_SITE_NO"));
					  te.setCATA_ID(rs.getString("CATA_ID"));
					  te.setAPPLY_SITE_NO(rs.getString("APPLY_SITE_NO"));
					  te.setPROVIDE_SITE_NO(rs.getString("PROV_DEPT_ID"));
					  //源数据库
					  te.setSOURCE_DB_IP(rs.getString("SOURCE_DB_IP"));
					  te.setSOURCE_DB_NAME(rs.getString("SOURCE_DB_SID")+"_SOURCE");					  
					  te.setSOURCE_DB_SCHEMA(rs.getString("SOURCE_DB_SID"));
					  te.setSOURCE_DATABASE_ENCREPTED_PASSWORD(rs.getString("SOURCE_DB_PASSWORD"));
					  if("1".equals(String.valueOf(rs.getString("SOURCE_DB_TYPE")))) {
						  te.setSOURCE_DB_TYPE("ORACLE");
						  te.setSOURCE_DB_PORT("1521");
					  }else if("2".equals(String.valueOf(rs.getString("SOURCE_DB_TYPE")))) {
						  te.setSOURCE_DB_TYPE("MYSQL");
						  te.setSOURCE_DB_PORT("3306");
					  }else if("3".equals(String.valueOf(rs.getString("SOURCE_DB_TYPE")))) {
						  te.setSOURCE_DB_TYPE("MSSSQL");
						  te.setSOURCE_DB_PORT("1433");
					  }else if("4".equals(String.valueOf(rs.getString("SOURCE_DB_TYPE")))) {
						  te.setSOURCE_DB_TYPE("POSTGRESQL");
						  te.setSOURCE_DB_PORT("5432");
					  }else if("5".equals(String.valueOf(rs.getString("SOURCE_DB_TYPE")))) {
						  te.setSOURCE_DB_TYPE("POSTGRESQL");
						  te.setSOURCE_DB_PORT("5432");
					  }
					  te.setSOURCE_DB_USERNAME(rs.getString("SOURCE_DB_USERNAME"));
					  String dbtype = rs.getString("DB_TYPE");
					  String[] dburl = rs.getString("DB_URL").split(":");
					  //目标数据库
					  if("1".equals(String.valueOf(dbtype))) {
						  te.setTARGET_DB_IP(dburl[3].substring(1));
						  te.setTARGET_DB_NAME(dburl[5]+"_TARGET");
						  te.setTARGET_DB_PORT("1521");
						  te.setTARGET_DB_TYPE("ORACLE");
						  te.setTARGET_DB_SCHEMA(dburl[5]);
					  }
					  if("2".equals(String.valueOf(dbtype))) {
						  te.setTARGET_DB_IP(dburl[2].substring(2));
						  te.setTARGET_DB_NAME(dburl[3].split("/")[1]+"_TARGET");
						  te.setTARGET_DB_PORT("3306");
						  te.setTARGET_DB_TYPE("MYSQL");
						  te.setTARGET_DB_SCHEMA(dburl[3].split("/")[1]);
					  }
					  if("3".equals(String.valueOf(dbtype))) {
						  te.setTARGET_DB_IP(dburl[3].substring(2));
						  te.setTARGET_DB_NAME(dburl[4].split("=")[1]+"_TARGET");
						  te.setTARGET_DB_PORT("1433");
						  te.setTARGET_DB_TYPE("MSSQL");
						  te.setTARGET_DB_SCHEMA(dburl[4].split("=")[1]);
					  }
					  if("4".equals(String.valueOf(dbtype))||"5".equals(String.valueOf(dbtype))) {
						  te.setTARGET_DB_IP(dburl[2].substring(2));
						  te.setTARGET_DB_NAME(dburl[3].split("=")[1]+"_TARGET");
						  te.setTARGET_DB_PORT("5432");
						  te.setTARGET_DB_TYPE("POSTGRESQL");
						  te.setTARGET_DB_SCHEMA(dburl[3].split("=")[1]);
					  }					  
					  te.setTARGET_DB_USERNAME(rs.getString("DB_USER"));
					  te.setTARGET_DATABASE_ENCREPTED_PASSWORD(rs.getString("DB_PASS"));
					  i=1;
				  }
				  stfs.append(rs.getString("INDEX_ENG_NAME"));
				  stfs.append(",");
				  ttfx.append("<value><name>");
				  ttfx.append(rs.getString("INDEX_ENG_NAME"));
				  ttfx.append("</name>");
				  ttfx.append("<rename>");
				  ttfx.append(rs.getString("INDEX_ENG_NAME"));
				  ttfx.append("</rename>");
				  ttfx.append("<update>Y</update></value>");
				  te.setSOURCE_TABLE_FIELDS_STR(stfs.toString());
				  te.setTARGET_TABLE_FIELDS_XML(ttfx.toString());
			  }
		  }catch(Exception e) {
			  e.printStackTrace();
		  }finally{
			  if(rs!=null) {
				  try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			  }
			  if(stmt!=null) {
				  try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			  }
		  }
		  return list;
	  }
	  
	  public void closeConnect(Connection con)
	  {
	    if (con != null) {
	      try
	      {
	        con.close();
	      }
	      catch (SQLException e)
	      {
	        e.printStackTrace();
	      }
	    }
	  }
	  
	  public static String getSerialNo()
	  {
	    String str = "0123456789";
	    Date date = new Date();
	    StringBuffer sb = new StringBuffer();
	    Random random = new Random();
	    for (int sf = 0; sf < 10; sf++) {
	      sb.append(str.charAt(random.nextInt(str.length())));
	    }
	    SimpleDateFormat arg5 = new SimpleDateFormat("yyyyMMddHHmmssS");
	    String id = String.valueOf(arg5.format(date) + sb.toString());
	    return id;
	  }
	  
	  public static void test(String[] args) {
		  try {
			KettleEnvironment.init();
			System.out.println(Encr.decryptPassword("2be98afc86aa78081b814a974dcc3f782"));
			System.out.println(Password.pWORD("CgyIme|5w3rop3o)?_G7H", 2));
		} catch (KettleException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
