package hbs;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.ProtocolSignature;

public class ExportImage {

	static HblockConnectionManager hbcManager = new HblockConnectionManager();
    static HBSv5 hbs;
    public HbsUtil utilities = new HbsUtil();
    public FileSystem fs = utilities.getFileSystem();

    public ImageOut exportImage(ImageId imgId) {
    	hbs = hbcManager.getBlockStore(imgId.id);
    	
        ImageOut iOut = new ImageOut();
        long addr = 0;//****;
        long fileIndex = addr/BlockProtocol.hadoopBlockSize;
        Path filePath = new Path(imgId.id +"/" + String.valueOf(fileIndex));
        try{
        	FSDataInputStream dis = fs.open(filePath);
        	iOut.dis = dis;
        	return iOut;
        } catch (Exception e) {
        	e.printStackTrace();
        	return null;
        }
    }
}