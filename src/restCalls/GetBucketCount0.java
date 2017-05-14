package restCalls;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import hbs.HbsUtil;
import hos.BucketInfo;
import hos.HadoopObjectStore;
import hos.ObjectId;
import hos.ObjectInfo;
import hos.ObjectOut;
import hos.UserId;

public class GetObject extends HttpServlet{

	  HbsUtil util = new HbsUtil();

	  public void doGet(HttpServletRequest request, HttpServletResponse response)
			  throws ServletException, IOException {
		  	String bucketKey = (String) request.getParameter("bucketKey");
	  		String objectKey = (String) request.getParameter("objectKey");
	  		String userName = (String) request.getParameter("userName");
	  		StringBuilder message = new StringBuilder ();
	  		ObjectOut objOutput = HadoopObjectStore.getHadoopObjectStore().GetObject(message , new BucketInfo(new UserId(userName), bucketKey), new ObjectId(objectKey));
	  		if(objOutput == null)
	  		{
				try{
					response.getWriter().write(message.toString());
				} catch (Exception e){
					e.printStackTrace();
				}
				return;
	  		}
	  		ObjectInfo objInfo = objOutput.objectInfo;
	  		DataInputStream din = objOutput.dis;

			response.setContentLength(objInfo.lenofFile);

		  	if (objInfo.contentType!=""){
			  	response.setContentType(objInfo.contentType);
		  	}
		  	if (objInfo.characterEnconding!=""){
		  		response.setCharacterEncoding(objInfo.characterEnconding);
		  	}
			        //response.setContentLength(-1);
//			response.setHeader("Content-Transfer-Encoding", "binary");
			response.setHeader("Content-Disposition","filename=\"" + objInfo.fileName + "\"");//fileName);
			response.setHeader("MetaData", objOutput.uMetaData);
			ServletOutputStream sos = response.getOutputStream();

			byte[] buffer = new byte[10240];
			for (int length=0; (length = din.read(buffer)) > 0;) {
				sos.write(buffer, 0, length);
			}

			sos.close();
			din.close();

	  }
}
