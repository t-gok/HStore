package hos;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.util.List;

import javax.servlet.ServletInputStream;

import org.apache.hadoop.ipc.VersionedProtocol;


public interface ObjectProtocol extends VersionedProtocol {

	public static final String smallFilesFolderName = "smallFiles";
	public static final long HADOOP_BLOCK_SIZE = 128<<20;
	public static final String FILENAME_SEPERATOR = ",";
	public static final String USER_BUCKETS_TABLE_NAME = "emp";
	public static final String USER_BUCKETS_TABLE_COL_FAM = "pData";
	public static final String USER_BUCKETS_TABLE_COL_1 = "name";
	public static final String USER_BUCKETS_TABLE_COL_2 = "nwe";
	public static final long ARCHIVE_SIZE_THRESHOLD = 500<<20;
	public static final double UNARCHIVE_PERCENT_THRESHOLD = 50.0;
	
	public static final long versionID = 1L;
	
	public List<String> ListBuckets(UserId userID);
	
	public String PutBucket(BucketInfo bucketID);
		
	public String DeleteBucket(BucketInfo bucketID);
	
	public List<String> ListObjects(BucketInfo bucketID);
	
	public String PutObject(BucketInfo bucketID, ObjectInfo objectInfo, ServletInputStream reader);
	
	public String DeleteObject(BucketInfo bucketID, ObjectId objectKey);
	
	public ObjectOut GetObject(StringBuilder message , BucketInfo bucketID,ObjectId objectKey);
	
}
