package restCalls;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import javax.el.MapELResolver;
import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;

import hbs.HbsUtil;
import hos.BucketACL;
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.ObjectInfo;
import hos.ObjectProtocol;
import hos.UserId;

public class PutBucketACL extends HttpServlet{
	
	HbsUtil util = new HbsUtil();
	
		public void doPut(HttpServletRequest request, HttpServletResponse response) {
			JSONObject resp = new JSONObject();

		  	String bucketKey = (String) request.getParameter("bucketKey");
		  	String userName = (String) request.getParameter("userName");
		  	String target = (String) request.getParameter("target");
		  	BucketACL checkP = new BucketACL();
		  	String message = checkP.insertBucketACL(new BucketInfo(new UserId(userName),bucketKey), target);
		  		  	
		  	resp.put("response", message);
	  		try {
				response.getWriter().write(resp.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	  
}
