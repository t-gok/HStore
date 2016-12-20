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
import hos.ObjectId;
import hos.UserId;

public class ListObjectsInBucket extends HttpServlet{

	  HbsUtil util = new HbsUtil();
	
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
	  	String bucketKey = (String) request.getParameter("bucketKey");
	  	String userName = (String) request.getParameter("userName");
	  	UserId userId = new UserId(userName); 
		  System.out.println(" starting getObjectList");
		  System.out.println(" starting getObjectList");
		  System.out.println(" starting getObjectList");
	  	List<String> bucketList =    HadoopObjectStore.getHadoopObjectStore().ListObjects(new BucketInfo(new UserId(userName),bucketKey));
	  	String message;
	  	if(bucketList == null){
	  		message = "Failed. Check UserName and BucketKey";
	  		try{
				response.getWriter().write(message.toString());				
			} catch (Exception e){
				e.printStackTrace();
			}
			return;
	  	}
	  	JSONObject resp = new JSONObject();
	  	JSONArray bucketValues = new JSONArray();
		 for (int i=0; i<bucketList.size(); i++){
			  JSONObject bucket = new JSONObject();
			  bucket.put(String.valueOf(i+1), bucketList.get(i));
			  bucketValues.add(bucket);
		  }
		  resp.put("objectList", bucketValues);
		  response.setContentType("application/json");
		  response.getWriter().write(resp.toString());;

	
	}
	
}
