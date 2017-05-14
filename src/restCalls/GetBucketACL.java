package restCalls;

import hbs.HbsUtil;
import hos.BucketACL;
import hos.BucketInfo;
import hos.UserId;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;

public class GetBucketACL {

	  HbsUtil util = new HbsUtil();
	
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
  throws ServletException, IOException {
			JSONObject resp = new JSONObject();

		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String userName = (String) request.getParameter("userName");
		  	String target = (String) request.getParameter("target");
		  	BucketACL checkP = new BucketACL();
		  	String message = checkP.getBucketACL(new BucketInfo(new UserId(userName),bucketKey),target);
		  		  	
		  	resp.put("response", message);
	  		try {
				response.getWriter().write(resp.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
