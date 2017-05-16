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

public class GetObjectCount extends HttpServlet{

	  HbsUtil util = new HbsUtil();

	  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
	  	String userName = (String) request.getParameter("userName");
	  	UserId userId = new UserId(userName);
			ArrayList<String> bucketList = (ArrayList<String>) HadoopObjectStore.getHadoopObjectStore().ListBuckets(new UserId(userName));
			int objectCount = 0;
			int totalSize = 0;
			for(int i=0; i<bucketList.size(); i++)
			{
				String bucketKey = bucketList.get(i);
				List<String> objectList =  HadoopObjectStore.getHadoopObjectStore().ListObjects(new BucketInfo(new UserId(userName),bucketKey));
				objectCount += objectList.size();
//				for (int j=0; j<objectList.size(); j++)
//				{
//						totalSize += objectList.get(j).lenofFile;
//				}
			}
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
			resp.put("objectCount", objectCount);
			resp.put("totalSize", totalSize);

			//FileSystem fs = util.getFileSystem();

			response.setContentType("application/json");
			response.getWriter().write(resp.toString());

	}

}
