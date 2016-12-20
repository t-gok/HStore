package hos;

import hbs.HbsUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Checks {
	public static String checkBucketInfo(BucketInfo bucketID){
		  final HbsUtil util = new HbsUtil();
		  FileSystem fs = util.getFileSystem();
		  Path appHomePath = new Path("/hos/");
		  try
		  {
			  if(!fs.exists(appHomePath))
				  fs.mkdirs(appHomePath);
		  }
		  catch(Exception e)
		  {
			  e.printStackTrace();
		  }
		String message = "";
		if(bucketID.getQueryUser().getId() == null || bucketID.getQueryUser().getId().isEmpty()){
			message += "Invalid user name, Please choose appropriate username";
			return message;
		}
		if(bucketID.getBucketName() == null || bucketID.getBucketName().isEmpty()){
			message += "Invalid bucket name, Please choose appropriate bucketname";
			return message;
		}
		if (bucketID.getBucketName().contains(",")){
			message = "Keys cannot contain ',' ";
			return message;
		}
		return message;
	}
	
	public static boolean bucketExists(BucketInfo binfo) {
		final HbsUtil util = new HbsUtil();
		FileSystem fs = util.getFileSystem();
		  Path appHomePath = new Path("/hos/");
		  try
		  {
			  if(!fs.exists(appHomePath))
				  fs.mkdirs(appHomePath);
		  }
		  catch(Exception e)
		  {
			  e.printStackTrace();
		  }
		try{
			if(fs.exists(new Path("/hos/")) && fs.exists(new Path("/hos/"+binfo.getBucketName()))  
			   ) return true;
			else
				 return false;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}
	public static String checkUserId(UserId userID){
		String message = "";
		if(userID.getId() == null || userID.getId().isEmpty()){
			message += "Invalid user name, Please choose appropriate username";
		}
		return message;
	}
}
