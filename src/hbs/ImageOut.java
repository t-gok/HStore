package hbs;

import java.io.DataInputStream;

public class ImageOut
{
	public ImageInfo imageInfo;
	public DataInputStream dis;
	
	public ImageOut(ImageInfo o, DataInputStream d){
		this.dis = d;
		this.imageInfo = o;
	}

	public ImageOut(){
		this.dis = null;
		this.imageInfo = null;
	}
}
