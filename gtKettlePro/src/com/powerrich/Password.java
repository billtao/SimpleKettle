package com.powerrich;

import java.io.UnsupportedEncodingException;

import it.sauronsoftware.base64.Base64;

public class Password {
	private static final String UTF_8 = "UTF-8";
	private static String str;
	
	public static String pWORD(String sSource, int iFlag) {
		if(sSource==null) {
			return null;
		}else {
			String ls_code="";
			int li_len,i;
			char li_asc;
			String ls_i;
			int li_ret;
			if (iFlag == 1)
		    {
		      li_len = sSource.length();
		      int li_head = (int)(Math.random() * 10.0D);
		      if (li_head == 0) {
		        li_head = 1;
		      }
		      for(i = 0; i < li_len; ++i) {
		    	  li_ret = (int)(Math.random() * 94.0D);
			      if (li_ret == 0) {
			        li_ret = 1;
			      }
			      int li_rand = li_ret + 32;
			      li_asc = sSource.substring(i, i + 1).charAt(0);
			      ls_i = String.valueOf((char)(li_asc - i));
			      if(li_asc+i+li_head >126) {
			          if (li_rand % 2 == 1) {
				            ++li_rand;
			          }
			          ls_i = (char)li_rand + String.valueOf((char)(li_asc - i - li_head));			    	  
			      }
			      ls_code = ls_code + ls_i;
		      }
		      li_ret = (int)(Math.random() * 9.0D);
		      if(li_ret == 0) {
		    	  li_ret = 1;
		      }
		      ls_code = (char)(li_ret * 10 + li_head + 40) + ls_code;
		    }else {
		    	li_len = sSource.length();
		    	ls_code = "";
		    	li_ret = sSource.substring(0,1).charAt(0)%10;
		    	for(i=2;i<li_len;i+=2) {
		    		li_asc = sSource.substring(i,i+1).charAt(0);
		    		if(sSource.substring(i-1,i).charAt(0)%2==0) {
		    			ls_i = String.valueOf((char)(li_asc+(i-1)/2 +li_ret));
		    		}else {
		    			ls_i = String.valueOf((char)(li_asc - (i - 1) / 2 - li_ret));
		    		}
		    		ls_code = ls_code + ls_i;
		    	}
		    }
			return ls_code;
		}
	}
	
	public static String encrypt(String data) throws UnsupportedEncodingException, RuntimeException {
		if (data == null) {
			return null;
		}
		byte[] b = Base64.encode(data.getBytes("UTF-8"));
		str = new String(b, "UTF-8");
		return str;
	}

	public static String decrypt(String data) throws UnsupportedEncodingException, RuntimeException {
		if (data == null) {
			return null;
		}
		byte[] b = Base64.decode(data.getBytes("UTF-8"));
		str = new String(b, "UTF-8");
		return str;
	}

}
