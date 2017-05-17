package hos;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletInputStream;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.ProtocolSignature;
import hbs.HbsUtil;
import restCalls.RestUtil;
import hbase.DBStrings;
import hbase.HbaseManager;
import hbase.HbaseUtil;
import hos.Checks;

public class HadoopObjectStore implements ObjectProtocol {

	public HbaseManager hbaseManager = null;
	public HbsUtil util = new HbsUtil();
	public HbaseUtil hbaseUtil = new HbaseUtil();
	public static String message;
	public static BucketACL permCheck = new BucketACL();
	public static ObjectACL objpermCheck = new ObjectACL();
	private static HadoopObjectStore defaultObjectStore = null;
	public static HadoopObjectStore getHadoopObjectStore(){
		if ( defaultObjectStore == null){
			defaultObjectStore = new HadoopObjectStore();
		}
		return defaultObjectStore;
	}
//	ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	public HadoopObjectStore() {
		hbaseManager = new HbaseManager();
//		executor.scheduleAtFixedRate(BackGroundTasks.getBackGroundTasks(), 3, 3, TimeUnit.SECONDS);
	}
	
	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> ListBuckets(UserId userID) {
		  // get entries with RestUtil.USER_NAME in userBuckets table and return
		 String check = Checks.checkUserId(userID);
		 if(check != ""){	// No User
			 return null;
		 }
		 HbaseUtil hbaseUtil = new HbaseUtil();
		 ArrayList<Result> res = hbaseUtil.getResultsForPrefix(DBStrings.Table_bucketsTableString, userID.getId());
		 ArrayList<String> ret = new ArrayList<String>();

		 String colFam = DBStrings.DEFAULT_COLUMN_FAMILY;
		 String col1 = DBStrings.Col_bucketID;
		 
		 if (res == null) return ret;
		 
		 for (int i=0; i<res.size(); i++){
			 Result curr = res.get(i);
			 String rowKey = Bytes.toString(curr.getRow());
			 String bucketValue = Bytes.toString(curr.getValue(Bytes.toBytes(colFam), Bytes.toBytes(col1)));
			 ret.add(bucketValue);
		 }
		  return ret;
	}

	private boolean folderAlreadyExists;


	public void errorlog(String content)
	{
		try {
			File file = new File("webapps/log.txt");
			content += "\n";
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String PutBucket(BucketInfo bucketID) {
		message = Checks.checkBucketInfo(bucketID);
		if(message != "") return message;
		folderAlreadyExists = false;
		boolean success = createBucketInFolder(bucketID);
	  	if (success){
	  		errorlog("pb1");
	  		addEntriesToUserBuckets(bucketID);
	  		errorlog("pb2");
	  		message = "Create Bucket is Successful";
			Path path = new Path("/hos/" + bucketID.getBucketName());
			List<AclEntry> newaclList = new ArrayList<AclEntry>();
		    newaclList.add(AclEntry.parseAclEntry("user::---",true));
		    newaclList.add(AclEntry.parseAclEntry("group::---",true));
		    newaclList.add(AclEntry.parseAclEntry("other::---",true));
		    newaclList.add(AclEntry.parseAclEntry("user:"+bucketID.getQueryUser().getId()+":"+"rwx",true));
			FileSystem fs = util.getFileSystem();
			try {
				fs.modifyAclEntries(path,newaclList);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	  	}
	  	else{
	  		message = "failed";
	  		errorlog("pb3");
		  	if (folderAlreadyExists){
		  		message += ". Bucket with same name is already present";
		  	}
		  	else{
		  		message += ". Error creating bucket";
		  	}
	  	}
  		errorlog("pb4");
		return message;
	}


	@Override
	public String DeleteBucket(BucketInfo binfo) {
		message = "";
		FsAction fa = permCheck.checkPermission(binfo);
		if(fa != FsAction.READ_WRITE && fa != FsAction.ALL)
			return "No sufficient permissions to delete bucket";
	/*
	 * TODO - check for small files in the bucket
	 * 	The deletebucketInFolder raises exception if there are files in it.
	 * 	since fs.delete(_, false) is called.
	 *  
	 * 	If the files are smallFiles, they will be stored in the smallFiles folder.
	 *  So, even if the folder is empty, the bucket may not be. Check in DB for this.
	 *  HbaseUtil has scanWithPrefix. Use it to fix this.  
	 * */
	
		boolean success = true, noSuchBucket = true , bucketNotEmpty = false;
		FileSystem fs = util.getFileSystem();
		Path p = new Path(util.genDefaultUserPath(binfo));
		try {
			if (fs.exists(p)){
				noSuchBucket = false;
				try{
					fs.delete(p, false); 						
					success = true;
				} catch (Exception e){
					bucketNotEmpty = true;
					success = false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			success = false;
		}
		  
	  	if (success){
	  		message += "Success";
	  		deleteEntriesForDeleteBucket(binfo);
	  	}
	  	else{
	  		message += "Delete Bucket failed";
	  		if (noSuchBucket){
	  			message += ". No such bucket exists";
	  		}
	  		if (bucketNotEmpty){
	  			message += ". Bucket is not Empty";
	  		}
	  	}
		return message;
	}

	@Override
	public List<String> ListObjects(BucketInfo binfo) {
		String message = Checks.checkBucketInfo(binfo);
		if(message != "" || !Checks.bucketExists(binfo)) return null;
		FsAction fa = permCheck.checkPermission(binfo);
		if(fa != FsAction.READ_WRITE && fa != FsAction.ALL && fa != FsAction.READ)
		{
			List<String> res = new ArrayList<String>();
			res.add("No sufficient permissions to read from bucket");
			return res;
		}
		return getObjectList(binfo.getQueryUser().getId() + "," + binfo.getBucketName() + ",");
	}

	@Override
	public String PutObject(BucketInfo binfo, ObjectInfo objectData, ServletInputStream reader) {
		
		String message = Checks.checkBucketInfo(binfo);
		if(message != "") return message;
		if(!Checks.bucketExists(binfo)) return "Failed. Bucket does not exist";
		if(objectData == null || reader == null)
			return "Failed. Please give proper object data";
		if (objectData.fileName.contains(",")){
			return "Failed. Key cannot contain ','";
		}
		FsAction fa = permCheck.checkPermission(binfo);
		if(fa != FsAction.READ_WRITE && fa != FsAction.ALL)
			return "No sufficient permissions to write to bucket";
		
		FileSystem fs = util.getFileSystem();
		String responseMessage = "";
		String fileName =  objectData.fileName;
		String path = getNewFilePath(binfo, fileName, isSmallFile(objectData.lenofFile));
		
		responseMessage += " len: " + objectData.lenofFile + " ";
		
		// For small files only: check for previous record and if it exists - add the previous entry to deleteTable
		
		Result r = HbaseUtil.getResult(DBStrings.Table_objectsTableString, binfo.getQueryUser().getId() + "," + binfo.getBucketName() + "," + objectData.fileName);
		if (!r.isEmpty()){
			String sizeOfFile = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_fileSize)));
			String cPath = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url)));
			if (isSmallFile(Integer.parseInt(sizeOfFile))){
				// add entry to small files delete table
				int indexOfSlash = cPath.lastIndexOf('/');
				String corrHDFSPath = "/hos/smallFiles" + cPath.substring(indexOfSlash);
				
				hbaseManager.AddRowinTable(hbaseManager.smallFilesDeleteTable, corrHDFSPath, new String[]{"1"}, new String[]{cPath});
			}
			else{
				try {
					errorlog(cPath);
					fs.delete(new Path(cPath), false);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		
		try{				
			
			Path p = new Path(path);
			objectData.path = path;
			
			if (fs.exists(p)){
				fs.delete(p, false);
			}
			
			FSDataOutputStream fsOut = fs.create(p);
			byte[] buffer = new byte[10240];
			for (int length = 0; (length = reader.read(buffer)) > 0;) {
	    			fsOut.write(buffer, 0, length);
    			}
			reader.close();
			
//			String line = null;
//			while((line=reader.readLine())!=null){
//				fsOut.writeBytes(line);
//				fsOut.writeBytes("\n");
//			}
			fsOut.close();

			List<AclEntry> newaclList = new ArrayList<AclEntry>();
			if(isSmallFile(objectData.lenofFile))
				newaclList.add(AclEntry.parseAclEntry("user:"+binfo.getQueryUser().getId()+","+fileName+":"+"rwx",true));
			else
				newaclList.add(AclEntry.parseAclEntry("user:"+binfo.getQueryUser().getId()+":"+"rwx",true));
		    fs.modifyAclEntries(p,newaclList);
			responseMessage += "success";
			addEntriesForPutObject(binfo, objectData);
		} catch (Exception e){
			responseMessage += ExceptionUtils.getStackTrace(e);
		}
		
		responseMessage = responseMessage + "path : "  + path + "\n";
		responseMessage = responseMessage + "name : "  + fileName + "\n";
		responseMessage = responseMessage + "encoding : "  + objectData.characterEnconding+ "\n";
		responseMessage = responseMessage + "contentType : "  + objectData.contentType+ "\n";
		
		// file upload is done
		
		
		
		return responseMessage;
	}

	@Override
	public String DeleteObject(BucketInfo binfo, ObjectId objectKey) {
		message = Checks.checkBucketInfo(binfo);
		if(message != "") return message;
		if(!Checks.bucketExists(binfo)) return ("Failed. Bucket does not exist");
		if(objectKey == null || objectKey.id == null || objectKey.id == "")
			return "Failed. Please select proper objectKey";

//		FsAction fa = objpermCheck.checkPermission(binfo , objectKey);
//		if(fa != FsAction.READ_WRITE && fa != FsAction.ALL)
//			return "No sufficient permissions to delete object";
		
		FileSystem fs = util.getFileSystem();
		Result r = HbaseUtil.getResult(DBStrings.Table_objectsTableString, binfo.getQueryUser().getId() + "," + binfo.getBucketName() + "," + objectKey.id);
		if (!r.isEmpty()){
			String sizeOfFile = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_fileSize)));
			String cPath = Bytes.toString(r.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_url)));
			if (isSmallFile(Integer.parseInt(sizeOfFile))){
				// add entry to small files delete table
				int indexOfSlash = cPath.lastIndexOf('/');
				String corrHDFSPath = "/hos/smallFiles" + cPath.substring(indexOfSlash);
				hbaseManager.AddRowinTable(hbaseManager.smallFilesDeleteTable, corrHDFSPath, new String[]{"1"}, new String[]{cPath});
				
				if (cPath.startsWith("har:")){
					String harPath = cPath.substring(0, indexOfSlash);
					int indexOfHarSlash = harPath.lastIndexOf('/');
					String harName = harPath.substring(indexOfHarSlash);
					String harHDFS = "/hos/harFiles" + harName;
					Result r2 = HbaseUtil.getResult(DBStrings.Table_harFilesTableString, harHDFS);
					if (r2.isEmpty()){
						// no entry in har table??
						// something wrong
					}
					else{
						String usedSpace = Bytes.toString(r2.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_validSpace)));
						String diskSpace = Bytes.toString(r2.getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_SpaceOnDisk)));
						
						long us= Long.parseLong(usedSpace);
						
						us -= Long.parseLong(sizeOfFile);
						usedSpace = String.valueOf(us);
						
						hbaseManager.AddRowinTable(hbaseManager.harFilesTable, harHDFS,
								new String[]{DBStrings.Col_validSpace, DBStrings.Col_SpaceOnDisk}, new String[]{usedSpace, diskSpace});
						
					}
				}
			}
			else{
				try {
					fs.delete(new Path(cPath), false);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			// delete entry from DB
		  	boolean part2Succ = deleteEntriesFromTable(binfo, objectKey.id);
		  	return (part2Succ ? ("Success") : ("Failed. Object not deleted from table"));
		}
		else{
			return "Failed. Object does not exist";
		}
		
	}

	@Override
	public ObjectOut GetObject(StringBuilder m , BucketInfo binfo, ObjectId objectKey) {
		// StringBuilder is used because a message 'm' is passed whenever there is error
		// String cannot be passes as reference, hence used StringBuilder
		m.append(Checks.checkBucketInfo(binfo));
		if(!m.toString().equals(""))
			return null;
		if(!Checks.bucketExists(binfo))
		{
			m.append("Bucket does not exist");
			return null;
		}
		if(objectKey == null || objectKey.id == null || objectKey.id == "")
		{
			m.append("Failed. Select proper objectkey");
			return null;
		}

//		FsAction fa = objpermCheck.checkPermission(binfo , objectKey);
//		if(fa != FsAction.READ_WRITE && fa != FsAction.ALL && fa != FsAction.READ)
//		{
//			m.append("You do not have sufficient permissions to read the object");
//			return null;
//		}
		
  		// get File path and its name
		ObjectInfoMetaData oimd = RestUtil.getObjectFromParams(binfo, objectKey.id); 
	  	ObjectInfo obj = oimd.objectInfo;
	  	String fileName  = obj.fileName;
	  	
	  	FSDataInputStream fsin  = null;
	  	
	  	String objPath = obj.path;
	  	Path p = new Path("none");
	  	if (objPath.startsWith("har:")){
	  		// the file is in a har file
	  		int lastIndex = objPath.lastIndexOf('/');
	  		String harPath = objPath.substring(0, lastIndex);
	  		
	  		try {
				p = new Path(new URI(objPath));
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		  	HarFileSystem hfs = util.getHarFileSystem();
		  	try {
				hfs.initialize(new URI(harPath),util.getConfiguration());
			  	fsin = hfs.open(new Path(objPath)); 
		  	} catch (Exception e) {
				e.printStackTrace();
			}
	  	}
	  	else{
	  		p = new Path(objPath);
		  	FileSystem fs = util.getFileSystem();
		  	
		  	try {
				fsin =fs.open(p);
			} catch (IOException e) {
				e.printStackTrace();
			}
	  	}
	  	
	  	
		DataInputStream din = new DataInputStream(fsin);
	  	
	  	return (new ObjectOut(obj, din, oimd.metaData));
	}

	
	
	/*
	 * the above uses below 
	 * */
	
	  private ArrayList<String> getObjectList(String DBPrefix){
		  
		  // get entries with RestUtil.USER_NAME_bucketKey in objectsTable and return 

		  System.out.println(" starting getObjectList");
		  ArrayList<Result> result = hbaseUtil.getResultsForPrefix(DBStrings.Table_objectsTableString, DBPrefix);
		  System.out.println("got results");
		  ArrayList<String> ret = new ArrayList<String>();
		  if (result == null) return ret;
		  for (int i=0; i<result.size(); i++){
			  String curr = Bytes.toString(result.get(i).getValue(Bytes.toBytes(DBStrings.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DBStrings.Col_ObjectID)));
			  ret.add(curr);
		  }
		  return ret;
	  }

	
	private String getNewFilePath(BucketInfo binfo, String oKey, boolean isSmall){
		String prefix = "/hos/";
		String suffix = "_" + HbaseUtil.getJuilianTimeStamp();
		if (isSmall){
			return  prefix + ObjectProtocol.smallFilesFolderName   + "/" + binfo.getBucketName() + ObjectProtocol.FILENAME_SEPERATOR + oKey + suffix;
		}
		return prefix +  binfo.getBucketName() + "/" + oKey + suffix ;
	}
	
	private boolean isSmallFile(int n){
		long frac = ((long)n*100)/ObjectProtocol.HADOOP_BLOCK_SIZE;
		if (frac<50){
			return true;
		}
		return false;
	}
	
	private boolean addEntriesForPutObject(BucketInfo binfo, ObjectInfo o){
		// add into objectsTable
		String rowKey = binfo.getQueryUser().getId() + "," + binfo.getBucketName() + "," + o.fileName;
		String colNames[] = new String[DBStrings.num_metadata];
		colNames[0] = DBStrings.Col_url;
		colNames[1] = DBStrings.Col_charEncoding;
		colNames[2] = DBStrings.Col_contentType;
		colNames[3] = DBStrings.Col_fileSize;
		colNames[4] = DBStrings.Col_userMetdata;
		colNames[5] = DBStrings.Col_ObjectID;
		colNames[6] = DBStrings.Col_timeStamp;

		StringBuffer sb = new StringBuffer();
		for (Entry<String,String[]> entry : o.uMetadata.entrySet()) {
			    sb.append(entry.getKey() + ":" + entry.getValue()[0] + "\n");
		}

		
		String colValues[] = new String[DBStrings.num_metadata];
		colValues[0] = o.path; 
		colValues[1] = o.characterEnconding;
		colValues[2] = o.contentType;
		colValues[3] = String.valueOf(o.lenofFile);
		colValues[4] = sb.toString();
		colValues[5] = o.fileName;
		colValues[6] = String.valueOf(System.currentTimeMillis());
		
		for (int i=0; i<DBStrings.num_metadata; i++){
			if (colValues[i] == null) colValues[i] = "";
		}
		
		hbaseManager.AddRowinTable(hbaseManager.objectsTable, rowKey, colNames, colValues);
		return true;
	}

	
	
	private boolean deleteEntriesFromTable(BucketInfo binfo, String objectKey){
		String rowKey = binfo.getQueryUser().getId() + "," + binfo.getBucketName() + "," + objectKey;
		hbaseManager.DeleteRowinTable(hbaseManager.objectsTable, rowKey);
		return true;
	}
  
	private boolean deleteObjectInBucket(String objPath){
	  	if (objPath.startsWith("har:")){
	  		return true;
	  	}
	  	else{
	  		FileSystem fs = util.getFileSystem();
	  		try {
				fs.delete(new Path(objPath), false);
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
	  		return false;
	  	}
	}
	
	public boolean createBucketInFolder(BucketInfo binfo){
		  folderAlreadyExists = false;
		  FileSystem fs = util.getFileSystem();
		  Path appHomePath = new Path("/hos/") , bucketPath = new Path("/hos/"+binfo.getBucketName());
		  try
		  {
			  if(!fs.exists(appHomePath))
				  fs.mkdirs(appHomePath);
			  if(!fs.exists(bucketPath))
				  fs.mkdirs(bucketPath);
			  return true;
		  }
		  catch(Exception e)
		  {
			  e.printStackTrace();
			  return false;
		  }
	  }
	  
	  public void addEntriesToUserBuckets(BucketInfo bucketKey){
		  
		  hbaseManager.AddRowinTable(hbaseManager.bucketsTable, bucketKey.getQueryUser().getId() + "," + bucketKey.getBucketName(), 
				  new String[]{DBStrings.Col_bucketID} , new String[]{bucketKey.getBucketName()});
	  }

	  private void deleteEntriesForDeleteBucket(BucketInfo binfo){
		  hbaseManager.DeleteRowinTable(hbaseManager.bucketsTable, binfo.getQueryUser().getId() + "," + binfo.getBucketName());
	  }
	
}
