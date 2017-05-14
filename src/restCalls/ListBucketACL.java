package restCalls;

import hbs.HbsUtil;
import hos.BucketACL;
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.UserId;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ListBucketACL {

	  HbsUtil util = new HbsUtil();
	
	  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
			JSONObject resp = new JSONObject();

		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String userName = (String) request.getParameter("userName");
		  	BucketACL checkP = new BucketACL();
		  	String message = checkP.listBucketACL(new BucketInfo(new UserId(userName),bucketKey));
		  		  	
		  	resp.put("response", message);
	  		try {
				response.getWriter().write(resp.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
