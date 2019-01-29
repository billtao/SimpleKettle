package com.powerrich;

public class JobEntity
{
  private String JOB_NAME;
  private String JOB_TRANSFER_NAME;
  private String JOB_TRANSFER_PATH;
  private String JOB_INTERVAL_SECONDS = "0";
  private String JOB_INTERVAL_MINUTES = "0";
  
  public void setJOB_NAME(String jOB_NAME)
  {
    this.JOB_NAME = jOB_NAME;
  }
  
  public void setJOB_TRANSFER_NAME(String jOB_TRANSFER_NAME)
  {
    this.JOB_TRANSFER_NAME = jOB_TRANSFER_NAME;
  }
  
  public void setJOB_TRANSFER_PATH(String jOB_TRANSFER_PATH)
  {
    this.JOB_TRANSFER_PATH = jOB_TRANSFER_PATH;
  }
  
  public String getJOB_INTERVAL_SECONDS()
  {
    return this.JOB_INTERVAL_SECONDS;
  }
  
  public void setJOB_INTERVAL_SECONDS(String jOB_INTERVAL_SECONDS)
  {
    this.JOB_INTERVAL_SECONDS = jOB_INTERVAL_SECONDS;
  }
  
  public String getJOB_INTERVAL_MINUTES()
  {
    return this.JOB_INTERVAL_MINUTES;
  }
  
  public void setJOB_INTERVAL_MINUTES(String jOB_INTERVAL_MINUTES)
  {
    this.JOB_INTERVAL_MINUTES = jOB_INTERVAL_MINUTES;
  }
  
  public String getJOB_NAME()
  {
    return this.JOB_NAME;
  }
  
  public String getJOB_TRANSFER_NAME()
  {
    return this.JOB_TRANSFER_NAME;
  }
  
  public String getJOB_TRANSFER_PATH()
  {
    return this.JOB_TRANSFER_PATH;
  }
}
