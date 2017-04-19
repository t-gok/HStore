package hos;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletInputStream;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.fs.permission.AclEntry;

import hbs.HbsUtil;

import restCalls.RestUtil;

import hbase.DBStrings;
import hbase.HbaseManager;
import hbase.HbaseUtil;

import hos.Checks;
public class ObjectACL {
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
	public FsAction checkPermission(BucketInfo binfo , ObjectId objectKey){
	//This function will return one of the following :
		//FsAction.NONE - user has no permission
		//FsAction.READ - user can read
		//FsAction.READ_WRITE - user can read and write
		//FsAction.ALL - user is the owner of the bucket
		ObjectInfoMetaData oimd = RestUtil.getObjectFromParams(binfo, objectKey.id); 
	  	ObjectInfo obj = oimd.objectInfo;
	  	String objPath = obj.path;
	  	Path p = null;
	  	if (objPath.startsWith("har:")){
	  		// the object is in a har file
	  		try{
	  		FileSystem fs = util.getFileSystem();
			p = new Path(new URI(objPath));
			//FileContext fileContext = FileContext.getFileContext();
			AclStatus aclStatus = fs.getAclStatus(p);
			List< AclEntry > aclList = aclStatus.getEntries();
			for(AclEntry temp : aclList)
				if( (binfo.getQueryUser().getId()+","+objectKey.id).equals(temp.getName()))
					return temp.getPermission();
	  		}
	  		catch(Exception e)
	  		{
	  			e.printStackTrace();
	  			return FsAction.NONE;
	  		}
	  	}
	  	else{
	  		try{
		  		p = new Path(objPath);
		  		FileSystem fs = util.getFileSystem();
				//FileContext fileContext = FileContext.getFileContext();
				AclStatus aclStatus = fs.getAclStatus(p);
				List< AclEntry > aclList = aclStatus.getEntries();
				for(AclEntry temp : aclList)
					if( (binfo.getQueryUser().getId()).equals(temp.getName()))
						return temp.getPermission();
	  		}
	  		catch(Exception e)
	  		{
	  			e.printStackTrace();
	  			return FsAction.NONE;
	  		}
	  	}
		return FsAction.NONE;
	}
	
	public String getObjectACL(BucketInfo binfo , ObjectId objectKey , String user)
	{
		String m = "";
		m = Checks.checkBucketInfo(binfo);
		if(!m.equals(""))
			return m;
		if(!Checks.bucketExists(binfo))
		{
			m = "Bucket does not exist";
			return m;
		}
		if(objectKey == null || objectKey.id == null || objectKey.id == "")
		{
			m = "Failed. Select proper objectkey";
			return m;
		}
		
		FsAction permission = checkPermission(binfo , objectKey);
		if(permission != FsAction.ALL)
			return "You do not have permission to get acl for this object"; 
		String answer = listObjectACL(binfo,objectKey);
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
	public boolean insertmodifyObjectACLUtil(BucketInfo binfo, ObjectId objectKey , Map<String,String> aclString , boolean mode){
		//mode == true implies insertion else modification
		ObjectInfoMetaData oimd = RestUtil.getObjectFromParams(binfo, objectKey.id);
	  	ObjectInfo obj = oimd.objectInfo;
	  	String objPath = obj.path;
	  	Path p = null;
	  	try{
		  	if (objPath.startsWith("har:")){
		  		// the file is in a har file
		  		FileSystem fs = util.getFileSystem();
				Path path = new Path(objPath);
				List<AclEntry> oldaclList = listObjectACLUtil(binfo,objectKey);
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
							if(temp.getName() == (userName+","+objectKey.id))
								return false;
						}
				    }
				    if(acl.equals("Reader"))
				    	to_be_added = "user:"+userName+","+objectKey.id+":"+"r--";
				    else if(acl.equals("Writer"))
				    	to_be_added = "user:"+userName+","+objectKey.id+":"+"rw-";
				    else if(acl.equals("Owner"))
				    	to_be_added = "user:"+userName+","+objectKey.id+":"+"rwx";
				    else if(acl.equals("None"))
				    	to_be_added = "user:"+userName+","+objectKey.id+":"+"---";
				    else
				    	return false;
				    newaclList.add(AclEntry.parseAclEntry(to_be_added,true));
					//FileContext fileContext = FileContext.getFileContext();
					//AclStatus aclStatus = fs.getAclStatus(path);
				}
				fs.modifyAclEntries(path,newaclList);
				return true;
		  	}
		  	else{
		  		FileSystem fs = util.getFileSystem();
				Path path = new Path(objPath);
				List<AclEntry> oldaclList = listObjectACLUtil(binfo,objectKey);
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
//				for(AclEntry temp : oldaclList)
//					if(temp.getName() in newaclList))
//				fs.setAcl(path, newaclList);
				fs.modifyAclEntries(path,newaclList);
				return true;
		  	}
	  	}
	  	catch(Exception e){
	  		e.printStackTrace();
	  		return false;
	  	}
		
		
	}

	public String insertObjectACL(BucketInfo binfo , String aclString , ObjectId objectKey)
	{
		String m = "";
		m = Checks.checkBucketInfo(binfo);
		if(!m.equals(""))
			return null;
		if(!Checks.bucketExists(binfo))
		{
			m = "Bucket does not exist";
			return m;
		}
		if(objectKey == null || objectKey.id == null || objectKey.id == "")
		{
			m = "Failed. Select proper objectkey";
			return m;
		}
		FsAction permission = checkPermission(binfo,objectKey);
		if(permission != FsAction.ALL)
			return "You do not have permission to change acl's for this object";
		Map<String,String> mp = new HashMap<String,String>();
		for(String eachAcl : aclString.split(","))
		{
			String rqd[] = eachAcl.split(":");
			if(rqd.length != 2)
				return "Failed. Check the format of request";
			mp.put(rqd[0], rqd[1]);
		}
		if(insertmodifyObjectACLUtil(binfo,objectKey,mp,true))
			return "Success";
		else
			return "Failed";
	}
	// insert and update are same functions
	public String updateObjectACL(BucketInfo binfo, String aclString,ObjectId objectKey)
	{
		String m = "";
		m = Checks.checkBucketInfo(binfo);
		if(!m.equals(""))
			return null;
		if(!Checks.bucketExists(binfo))
		{
			m = "Bucket does not exist";
			return m;
		}
		if(objectKey == null || objectKey.id == null || objectKey.id == "")
		{
			m = "Failed. Select proper objectkey";
			return m;
		}
		FsAction permission = checkPermission(binfo,objectKey);
		if(permission != FsAction.ALL)
			return "You do not have permission to change acl's for this object";
		Map<String,String> mp = new HashMap<String,String>();
		for(String eachAcl : aclString.split(","))
		{
			String rqd[] = eachAcl.split(":");
			if(rqd.length != 2)
				return "Failed. Check the format of request";
			mp.put(rqd[0], rqd[1]);
		}
		if(insertmodifyObjectACLUtil(binfo,objectKey,mp,false))
			return "Success";
		else
			return "Failed";
	}

	// how to check the owner of this path
	public boolean deleteObjectACLUtil(BucketInfo binfo, List<String> users , ObjectId objectKey)
	{
		try{
			ObjectInfoMetaData oimd = RestUtil.getObjectFromParams(binfo, objectKey.id); 
		  	ObjectInfo obj = oimd.objectInfo;
		  	String objPath = obj.path;
		  	Path p = new Path("none");
		  	if (objPath.startsWith("har:")){
		  		// the file is in a har file
				FileSystem fs = util.getFileSystem();
				Path path = new Path(objPath);
				List<AclEntry> oldaclList = listObjectACLUtil(binfo,objectKey);
				List<AclEntry> newaclList = new ArrayList<AclEntry>();
				for(AclEntry acl : oldaclList)
				{
					if(users.contains(acl.getName().split(",")[0]))
						newaclList.add(acl);
				}
				fs.removeAclEntries(path,newaclList);
				return true;
		  	}
		  	else{
				FileSystem fs = util.getFileSystem();
				Path path = new Path(objPath);
				List<AclEntry> oldaclList = listObjectACLUtil(binfo,objectKey);
				List<AclEntry> newaclList = new ArrayList<AclEntry>();
				for(AclEntry acl : oldaclList)
				{
					if(users.contains(acl.getName()))
						newaclList.add(acl);
				}
				fs.removeAclEntries(path,newaclList);
				return true;
		  	}
	  	}
	  	catch(Exception e)
	  	{
	  		e.printStackTrace();
	  		return false;
	  	}
	}
	
	public String deleteObjectACL(BucketInfo binfo , List<String> users , ObjectId objectKey)
	{
		String m = "";
		m = Checks.checkBucketInfo(binfo);
		if(!m.equals(""))
			return null;
		if(!Checks.bucketExists(binfo))
		{
			m = "Bucket does not exist";
			return m;
		}
		if(objectKey == null || objectKey.id == null || objectKey.id == "")
		{
			m = "Failed. Select proper objectkey";
			return m;
		}
		FsAction permission = checkPermission(binfo,objectKey);
		if(permission != FsAction.ALL)
			return "You do not have permission to change acl's for this object";
		if(deleteObjectACLUtil(binfo,users,objectKey))
			return "Success";
		else
			return "Failed";
	}

	public List<AclEntry> listObjectACLUtil(BucketInfo binfo , ObjectId objectKey){
		//the user can get the object acl only if he is the owner of the object
		//so checkpermission should return FsAction.ALL for this
		try{
			ObjectInfoMetaData oimd = RestUtil.getObjectFromParams(binfo, objectKey.id); 
		  	ObjectInfo obj = oimd.objectInfo;
		  	String objPath = obj.path;
		  	if (objPath.startsWith("har:")){
		  		// the file is in a har file
				FileSystem fs = util.getFileSystem();
				Path path = new Path(objPath);
				AclStatus aclStatus = fs.getAclStatus(path);
				List<AclEntry> temp = aclStatus.getEntries();
				List<AclEntry> to_ret = new ArrayList<AclEntry>();
				for(AclEntry ent : temp)
				{
					if(ent.getName().contains(objectKey.id))
						to_ret.add(ent);
				}
				return to_ret;
		  	}
		  	else{
				FileSystem fs = util.getFileSystem();
				Path path = new Path(objPath);
				AclStatus aclStatus = fs.getAclStatus(path);
				return aclStatus.getEntries();
		  	}
	  	}
	  	catch(Exception e)
	  	{
	  		e.printStackTrace();
	  		return null;
	  	}
	}
	
	public String listObjectACL(BucketInfo binfo,ObjectId objectKey)
	{
		FsAction permission = checkPermission(binfo,objectKey);
		if(permission != FsAction.ALL)
			return "You do not have permission to change acl's for this object";
		List<AclEntry> aclList = listObjectACLUtil(binfo,objectKey);
		if(aclList == null)
			return "";
		StringBuilder message = new StringBuilder("");
		for(AclEntry temp : aclList)
			if(!temp.getName().isEmpty())
				message.append(temp.getName().split(",")[0] + ":" + getRole(temp.getPermission()) + ",");
		return message.toString();
	}
}