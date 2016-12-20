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

import javax.servlet.ServletInputStream;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.ProtocolSignature;

public class ImportImage
{
	static HblockConnectionManager hbcManager = new HblockConnectionManager();
    static HBSv5 hbs = hbcManager.createNewBlockStore();
    public String importImage(ServletInputStream reader, ImageInfo iInfo)
    {    
        String responseMessage = "";
        long addr = 0;
        byte[] buffer = new byte[(int)BlockProtocol.clientBlockSize];
        try {
            String imageID = hbs.createImage(1<<30, HBSv5Test.clientBlockSize, HBSv5Test.hadoopBlockSize);// iInfo.lenofFile);
            BlockData blockData = new BlockData();

            for(int length = 0; (length = reader.read(buffer)) > 0;) {
                blockData.addr = addr;//****;
                blockData.value = buffer;
                hbs.writeBlock(iInfo.key, blockData.addr, blockData);
                addr += BlockProtocol.clientBlockSize;
            }
            hbs.commit(iInfo.key);
            responseMessage += "Successful";
        }
        catch (Exception e){
            responseMessage += "Exception: ";
            responseMessage += ExceptionUtils.getStackTrace(e);
        }
        return responseMessage;
    }
}