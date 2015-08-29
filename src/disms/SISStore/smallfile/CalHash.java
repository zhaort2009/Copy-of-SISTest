package disms.SISStore.smallfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CalHash{
	public static byte[] calcuMD5(File file) throws NoSuchAlgorithmException, IOException, DigestException{
		try {
		     MessageDigest messageDigest = MessageDigest.getInstance("MD5");
		     FileInputStream fis = new FileInputStream(file);
		     FileChannel ch = fis.getChannel(); 
		     MappedByteBuffer byteBuffer = ch.map(FileChannel.MapMode.READ_ONLY, 0, file.length()); 
		     messageDigest.update(byteBuffer); 
		     return messageDigest.digest(); 
		   } 
		catch (IOException e){
			return null; 
		 }	
	}
	public static String calHexString(byte[] b) {
		String hex = new String();
		String hexStr = new String();
		for (int i = 0; i < b.length; i++) { 
		     hex = Integer.toHexString(b[i] & 0xFF); 
		     if (hex.length() == 1) 
		     hex = '0' + hex; 
		     hexStr += hex;
		} 
		return hexStr.toUpperCase();
	}
	public static void main(String args[]) throws Exception
    {
		String filePath = "D:\\table.zip";
		File f = new File(filePath);
		byte[] hash = calcuMD5(f);
		System.out.println(calHexString(hash));
    }
}