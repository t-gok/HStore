package hos;

import hbs.HbsUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;


public class BucketACL {
	public static HbsUtil util = new HbsUtil();	
	
	String getRole(FsAction fa)
	{
		if(fa == FsAction.READ)
			return "Reader";
		else if(fa == FsAction.READ_WRITE)
			return "Writer";
		else if(fa == FsAction.ALL)
			return "Owner";
		else
			return "None";
	}
	
	FsAction getAction(String role)
	{
		if(role == "Reader")
			return FsAction.READ;
		else if(role == "Writer")
			return FsAction.READ_WRITE;
		else if(role == "Owner")
			return FsAction.ALL;
		else
			return FsAction.NONE;
	}
	// checker function to check the permissions of a user
	public FsAction checkPermission(BucketInfo binfo){
	//This function will return one of the following :
		//FsAction.NONE - user has no permission
		//FsAction.READ - user can read
		//FsAction.READ_WRITE - user can read and write
		//FsAction.ALL - user is the owner of the bucket
		try {
			FileSystem fs = util.getFileSystem();
			Path path = new Path("/hos/" + binfo.getBucketName());
			//FileContext fileContext = FileContext.getFileContext();
			AclStatus aclStatus = fs.getAclStatus(path);
			List< AclEntry > aclList = aclStatus.getEntries();
			for(AclEntry temp : aclList) {
				if(binfo.getQueryUser().getId().equals(temp.getName()))
					return temp.getPermission();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return FsAction.NONE;
	}
	
	public String getBucketACL(BucketInfo binfo , String user)
	{
		String message;
		message = Checks.checkBucketInfo(binfo);
		if(!message.isEmpty())
			return message;
		if(!Checks.bucketExists(binfo))
			return "Bucket does not exist";
		FsAction permission = checkPermission(binfo);
		if(permission != FsAction.ALL)
			return "You do not have permission to get acl for this bucket"; 
		String answer = listBucketACL(binfo);
		for(String each : answer.split(","))
		{
			String temp[] = each.split(":");
			if(temp[0] == user)
				return temp[1];
		}
		return "User specific role is not present";
	}

	//Write a single function for update and insert, handle it accordingly by using the single util function below

	// how to check the owner of this path
	public boolean insertmodifyBucketACLUtil(BucketInfo binfo, Map<String,String> aclString , boolean mode){
		//mode == true implies insertion else modification
		try {
			FileSystem fs = util.getFileSystem();
			Path path = new Path("/hos/" + binfo.getBucketName());
			List<AclEntry> oldaclList = listBucketACLUtil(binfo);
			List<AclEntry> newaclList = new ArrayList<AclEntry>();
			String to_be_added = "";
			for (Map.Entry<String, String> entry : aclString.entrySet())
			{
			    String userName = entry.getKey();
			    String acl = entry.getValue();
			    if(mode)
			    {
					for(AclEntry temp : oldaclList)
					{
						if(temp.getName() == userName)
							return false;
					}
			    }
			    if(acl.equals("Reader"))
			    	to_be_added = "user:"+userName+":"+"r--";
			    else if(acl.equals("Writer"))
			    	to_be_added = "user:"+userName+":"+"rw-";
			    else if(acl.equals("Owner"))
			    	to_be_added = "user:"+userName+":"+"rwx";
			    else if(acl.equals("None"))
			    	to_be_added = "user:"+userName+":"+"---";
			    else
			    	return false;
			    newaclList.add(AclEntry.parseAclEntry(to_be_added,true));
				//FileContext fileContext = FileContext.getFileContext();
				//AclStatus aclStatus = fs.getAclStatus(path);
			}
			fs.modifyAclEntries(path,newaclList);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public String insertBucketACL(BucketInfo binfo , String aclString)
	{
		String message;
		message = Checks.checkBucketInfo(binfo);
		if(!message.isEmpty())
			return message;
		if(!Checks.bucketExists(binfo))
			return "Bucket does not exist";
		FsAction permission = checkPermission(binfo);
		if(permission != FsAction.ALL)
			return "You do not have permission to change acl's for this bucket";
		Map<String,String> mp = new HashMap<String,String>();
		for(String eachAcl : aclString.split(","))
		{
			String rqd[] = eachAcl.split(":");
			if(rqd.length != 2)
				return "Failed. Check the format of request";
			mp.put(rqd[0], rqd[1]);
		}
		if(insertmodifyBucketACLUtil(binfo, mp,true))
			return "Success";
		else
			return "Failed";
	}
	// insert and update are same functions
	public String updateBucketACL(BucketInfo binfo, String aclString)
	{
		String message;
		message = Checks.checkBucketInfo(binfo);
		if(!message.isEmpty())
			return message;
		if(!Checks.bucketExists(binfo))
			return "Bucket does not exist";
		FsAction permission = checkPermission(binfo);
		if(permission != FsAction.ALL)
			return "You do not have permission to change acl's for this bucket";
		Map<String,String> mp = new HashMap<String,String>();
		for(String eachAcl : aclString.split(","))
		{
			String rqd[] = eachAcl.split(":");
			if(rqd.length != 2)
				return "Failed. Check the format of request";
			mp.put(rqd[0], rqd[1]);
		}
		if(insertmodifyBucketACLUtil(binfo, mp,false))
			return "Success";
		else
			return "Failed";
	}

	// how to check the owner of this path
	public boolean deleteBucketACLUtil(BucketInfo binfo, List<String> users)
	{
		try {
			FileSystem fs = util.getFileSystem();
			Path path = new Path("/hos/" + binfo.getBucketName());
			List<AclEntry> oldaclList = listBucketACLUtil(binfo);
			List<AclEntry> newaclList = new ArrayList<AclEntry>();
			for(AclEntry acl : oldaclList)
			{
				if(users.contains(acl.getName()))
					newaclList.add(acl);
			}
			fs.removeAclEntries(path,newaclList);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public String deleteBucketACL(BucketInfo binfo , List<String> users)
	{
		String message;
		message = Checks.checkBucketInfo(binfo);
		if(!message.isEmpty())
			return message;
		if(!Checks.bucketExists(binfo))
			return "Bucket does not exist";
		FsAction permission = checkPermission(binfo);
		if(permission != FsAction.ALL)
			return "You do not have permission to delete acl's for this bucket";
		if(deleteBucketACLUtil(binfo,users))
			return "Success";
		else
			return "Failed";
	}

	public List<AclEntry> listBucketACLUtil(BucketInfo binfo){
		//the user can get the bucket acl only if he is the owner of the bucket
		//so checkpermission should return FsAction.ALL for this
		try {
			FileSystem fs = util.getFileSystem();
			Path path = new Path("/hos/" + binfo.getBucketName());
			AclStatus aclStatus = fs.getAclStatus(path);
			return aclStatus.getEntries();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public String listBucketACL(BucketInfo binfo)
	{
		String m;
		m = Checks.checkBucketInfo(binfo);
		if(!m.isEmpty())
			return m;
		if(!Checks.bucketExists(binfo))
			return "Bucket does not exist";
		FsAction permission = checkPermission(binfo);
		if(permission != FsAction.ALL)
			return "You do not have permission to read acl's for this bucket"; 
		List<AclEntry> aclList = listBucketACLUtil(binfo);
		if(aclList == null)
			return "";
		StringBuilder message = new StringBuilder("");
		for(AclEntry temp : aclList)
			if(!temp.getName().isEmpty())
				message.append(temp.getName() + ":" + getRole(temp.getPermission()) + ",");
		return message.toString();
	}
}