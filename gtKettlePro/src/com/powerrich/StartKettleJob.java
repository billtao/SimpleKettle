package com.powerrich;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

public class StartKettleJob
{
  public static void main(String[] args)
  {
	Util kettleUtil = new Util();
    try
    {
      KettleEnvironment.init();
      
      List<String> list = getList(kettleUtil.kettleJobDir);
      int i = 0;
      do
      {
        JobMeta jm = new JobMeta((String)list.get(i), null);
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
