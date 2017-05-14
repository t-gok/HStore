package restCalls;

import hbs.HbsUtil;
import hos.BucketACL;
import hos.BucketInfo;
import hos.UserId;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;

public class UpdateBucketACL {

	HbsUtil util = new HbsUtil();
	
		public void doPut(HttpServletRequest request, HttpServletResponse response) {
			JSONObject resp = new JSONObject();

		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String userName = (String) request.getParameter("userName");
		  	String target = (String) request.getParameter("target");
		  	BucketACL checkP = new BucketACL();
		  	String message = checkP.updateBucketACL(new BucketInfo(new UserId(userName),bucketKey), target);
		  		  	
		  	resp.put("response", message);
	  		try {
				response.getWriter().write(resp.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	  
}
