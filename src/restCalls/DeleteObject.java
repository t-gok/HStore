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

import hbs.HbsUtil;
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.ObjectId;
import hos.UserId;

public class DeleteObject extends HttpServlet{

	  HbsUtil util = new HbsUtil();
	
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String objectKey = (String) request.getParameter("objectKey");
		  	String userName = (String) request.getParameter("userName");

		  	String message =  HadoopObjectStore.getHadoopObjectStore().DeleteObject(new BucketInfo(new UserId(userName) , bucketKey), new ObjectId(objectKey) );
		  	JSONObject resp = new JSONObject();
		  	resp.put("response", message);
	  		response.getWriter().write(resp.toString());
	  		
	  }
	  
}
