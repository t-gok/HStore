package hos;

public class BucketInfo {
	private String  bucketName = null;
	private UserId queryUser = null; //The type is userid because additional fields can be added for the user
	
	public BucketInfo(UserId uId, String bName){
		this.queryUser = uId;
		this.bucketName = bName;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public UserId getQueryUser() {
		return queryUser;
	}
}
