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

public class DeleteBucket extends HttpServlet{

	  HbsUtil util = new HbsUtil();
	  String message ;
	  
	  public void doDelete(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String userName = (String) request.getParameter("userName");
		  	message = "";
		  	JSONObject resp = new JSONObject();
		  	HadoopObjectStore h = HadoopObjectStore.getHadoopObjectStore();
		  	BucketInfo b = new BucketInfo(new UserId(userName) , bucketKey);
		  	message = h.DeleteBucket(b);
		  	resp.put("response", message);
	  		response.getWriter().write(resp.toString());
	  }
	  
	  
	
}
