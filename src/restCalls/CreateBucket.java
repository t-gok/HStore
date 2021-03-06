package restCalls;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import hbase.DBStrings;
import hbs.HbsUtil;
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.UserId;

public class CreateBucket extends HttpServlet{

	  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	HbsUtil util = new HbsUtil();
	boolean folderAlreadyExists; 
	
	  public void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
	  	JSONObject resp = new JSONObject();

	  	String bucketKey = (String) request.getParameter("bucketKey");
	  	String userName = (String) request.getParameter("userName");
	  	HadoopObjectStore h = HadoopObjectStore.getHadoopObjectStore(); 
	  	BucketInfo b = new BucketInfo(new UserId(userName), bucketKey); //BucketInfo(UserId uId, String bName)
	  	String message = h.PutBucket(b);
	  		  	
	  	resp.put("response", message);
  		response.getWriter().write(resp.toString());	  		
	  }
	
}
