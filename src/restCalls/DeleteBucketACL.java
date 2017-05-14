package restCalls;

import hbs.HbsUtil;
import hos.BucketACL;
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.UserId;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;

public class DeleteBucketACL {

	  HbsUtil util = new HbsUtil();
	  String message ;
	  
	  public void doDelete(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String userName = (String) request.getParameter("userName");
		  	String target = (String) request.getParameter("target");
		  	String[] users = target.split(",");
		  	BucketACL checkP = new BucketACL();
		  	String message = checkP.deleteBucketACL(new BucketInfo(new UserId(userName),bucketKey),Arrays.asList(users));
		  	
		  	message = "";
		  	JSONObject resp = new JSONObject();
		  	HadoopObjectStore h = HadoopObjectStore.getHadoopObjectStore();
		  	BucketInfo b = new BucketInfo(new UserId(userName) , bucketKey);
		  	message = h.DeleteBucket(b);
		  	resp.put("response", message);
	  		response.getWriter().write(resp.toString());
	  }
	  
}
