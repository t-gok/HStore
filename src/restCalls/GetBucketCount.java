package restCalls;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import hbase.DBStrings;
import hbase.HbaseUtil;
import hbs.HbsUtil;
import hos.HadoopObjectStore;
import hos.ObjectProtocol;
import hos.UserId;

public class GetBucketCount extends HttpServlet{

	HbsUtil util = new HbsUtil();

	public void doGet(HttpServletRequest request, HttpServletResponse response)
		throws ServletException, IOException {

		String userName = (String) request.getParameter("userName");
		ArrayList<String> bucketList = (ArrayList<String>) HadoopObjectStore.getHadoopObjectStore().ListBuckets(new UserId(userName));
		JSONObject resp = new JSONObject();
		resp.put("User", userName);
		resp.put("bucketCount", bucketList.size());

		//FileSystem fs = util.getFileSystem();

		response.setContentType("application/json");
		response.getWriter().write(resp.toString());
}

}
