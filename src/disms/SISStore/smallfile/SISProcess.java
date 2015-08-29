package disms.SISStore.smallfile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.Vector;

import javax.swing.JFileChooser;

import org.apache.commons.io.FileUtils;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import org.apache.commons.io.FileUtils;   
import org.apache.commons.io.FilenameUtils;   

public class SISProcess{
	Environment srcDbEnvironment = null;
	static Database srcDatabase= null;
	EnvironmentConfig srcConfig = null;
	DatabaseConfig srcDBConfig = null;
	Environment desDbEnvironment = null;
	static Database desDatabase = null;
	EnvironmentConfig desConfig = null;
	DatabaseConfig desDBConfig = null;
	String filePath = new String();
	static String srcDB = "D:/SIS/dbEnv";
	static String desDB = "E:/SIS/dbEnv";
	boolean exist = false;
	public void createSrcDB(String dbPath){
			try {    
			    // Open the environment. Create it if it does not already exist.    
			    srcConfig = new EnvironmentConfig();    
			    srcConfig.setAllowCreate(true); 
			    srcConfig.setTransactional(true);
			    srcConfig.setDurability(Durability.COMMIT_SYNC);
			    srcDbEnvironment = new Environment(new File(dbPath), srcConfig);    
			    
			    // Open the database. Create it if it does not already exist.    
			    srcDBConfig = new DatabaseConfig();    
			    srcDBConfig.setAllowCreate(true); 

			    srcDatabase = srcDbEnvironment.openDatabase(null,"sampleDatabase", srcDBConfig); 
			} 
			catch (DatabaseException dbe) {    // Exception handling goes here}
			}
	}
	public void createDesDB(String dbPath){
		try {    
		    // Open the environment. Create it if it does not already exist.    
		    desConfig = new EnvironmentConfig();    
		    desConfig.setAllowCreate(true); 
		    desConfig.setTransactional(true);
		    desConfig.setDurability(Durability.COMMIT_SYNC);
		    desDbEnvironment = new Environment(new File(dbPath), desConfig);    
		    
		    // Open the database. Create it if it does not already exist.    
		    desDBConfig = new DatabaseConfig();    
		    desDBConfig.setAllowCreate(true); 

		    desDatabase = desDbEnvironment.openDatabase(null,"sampleDatabase", desDBConfig); 
		} 
		catch (DatabaseException dbe) {    // Exception handling goes here}
		}
}
	public void closeSrcDB(){
			try {
		        if (srcDatabase != null) {
		            srcDatabase.close();
		        }
		        if (srcDbEnvironment != null) {
		            srcDbEnvironment.close();
		        }
		} catch (DatabaseException dbe) {
		    // 错误处理
		}
	}
	public void closeDesDB(){
		try {
	        if (desDatabase != null) {
	            desDatabase.close();
	        }
	        if (desDbEnvironment != null) {
	            desDbEnvironment.close();
	        }
	} catch (DatabaseException dbe) {
	    // 错误处理
	}
}
	public void cursorDB(){
		Cursor cursor = null;
		 // Open the cursor. 
		try {

	    cursor = desDatabase.openCursor(null, null);

	    // Cursors need a pair of DatabaseEntry objects to operate. These hold
	    // the key and data found at any given position in the database.
	    DatabaseEntry foundKey = new DatabaseEntry();
	    DatabaseEntry foundData = new DatabaseEntry();

	    // To iterate, just call getNext() until the last database record has been 
	    // read. All cursor operations return an OperationStatus, so just read 
	    // until we no longer see OperationStatus.SUCCESS
	    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
	        OperationStatus.SUCCESS) {
	        // getData() on the DatabaseEntry objects returns the byte array
	        // held by that object. We use this to get a String value. If the
	        // DatabaseEntry held a byte array representation of some other data
	        // type (such as a complex object) then this operation would look 
	        // considerably different.
	        String keyString = new String(foundKey.getData());
	        String dataString = new String(foundData.getData());
	        deletePairs(keyString);
	        System.out.println("Key - Data : " + keyString + " - " + dataString + "");
	    }
	} catch (DatabaseException de) {
	    System.err.println("Error accessing database." + de);
	} finally {
	    // Cursors must be closed.
	    cursor.close();
	}
	}
	public void addSrcElement(String key,String data){
		String aKey = key;
		String aData = data;
		try {
		DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
		DatabaseEntry theData = new DatabaseEntry(aData.getBytes("UTF-8"));
		srcDatabase.put(null, theKey, theData);
		} catch (Exception e) {
		// Exception handling goes here
		}
		
	}
	public void addDesElement(String key,String data){
		String aKey = key;
		String aData = data;
		try {
		DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
		DatabaseEntry theData = new DatabaseEntry(aData.getBytes("UTF-8"));
		desDatabase.put(null, theKey, theData);
		} catch (Exception e) {
		// Exception handling goes here
		}
		
	}
	public void deletePairs(String key){
		try {
		    String aKey = key;
		    DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
		    
		    // Perform the deletion. All records that use this key are
		    // deleted.
		    srcDatabase.delete(null, theKey); 
		    System.out.println("delete successfully!");
		} catch (Exception e) {
		    // Exception handling goes here
		}
	}
	public String getSrcValue(String key){
		 try { 
		      // Create a pair of DatabaseEntry objects. theKey
		      // is used to perform the search. theData is used 
		      // to store the data returned by the get() operation.
		     DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
		     DatabaseEntry theData = new DatabaseEntry();
		      
		     // Perform the get.
		      if (srcDatabase.get(null, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) { 
		            // Recreate the data String.
		             byte[] retData = theData.getData();
		             String foundData = new String(retData);
//		             System.out.println("For key: '" + key + "' found data: '" + foundData + "'.");
		             return foundData;
		      } else {
//		          System.out.println("No record found for key '" + key + "'."); 
		          return  null;
		     }
		  } catch (Exception e) {
			  // Exception handling goes here}
			  return null;
		  }
	}
	public static String getDesValue(String key){
		 try { 
		      // Create a pair of DatabaseEntry objects. theKey
		      // is used to perform the search. theData is used 
		      // to store the data returned by the get() operation.
		     DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
		     DatabaseEntry theData = new DatabaseEntry();
		      
		     // Perform the get.
		      if (desDatabase.get(null, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) { 
		            // Recreate the data String.
		             byte[] retData = theData.getData();
		             String foundData = new String(retData);
//		             System.out.println("For key: '" + key + "' found data: '" + foundData + "'.");
		             return foundData;
		      } else {
//		          System.out.println("No record found for key '" + key + "'."); 
		          return  null;
		     }
		  } catch (Exception e) {
			  // Exception handling goes here}
			  return null;
		  }
	}
	public void chooseFile(Vector fs){
		JFileChooser jfc = new JFileChooser("E:/test/linux/");
		jfc.setMultiSelectionEnabled(true);
		jfc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
		jfc.showOpenDialog(new javax.swing.JFrame());
		File[] sf = jfc.getSelectedFiles();
		for(int i = 0; i < sf.length;i++ ){
			if(!sf[i].isDirectory())
				fs.addElement(sf[i]);
			else
				fileList(sf[i],fs);
		}
		System.out.println(fs);
	}
	public void fileList(File file,Vector vt) {
        File[] files = file.listFiles();
        if (files != null) {
              for (File f : files) {
            	  if(!f.isDirectory())
//                    System.out.println(f.getPath());
            	  	vt.addElement(f.getPath());
                    fileList(f,vt);
              }
        }
   }
	
	public static void SISgroup() throws NoSuchAlgorithmException, DigestException, IOException{
		Vector fs = new Vector();
		SISProcess sp = new SISProcess();
		sp.chooseFile(fs);	
		String srcDB = "D:/SIS/dbEnv";
		String desDB = "E:/SIS/dbEnv";
		
		sp.createSrcDB(srcDB);
		sp.createDesDB(desDB);
		String srcFile ;
		File src ;
		long srcSize ;
		String desFile ;
		File des ;
		long desSize ;
		String location ;
		byte[] hash ;
		String hashStr ;
		String luid ;	
		desFile = "E:/SIS/ALL.txt";
		des = new File(desFile);
		FileOutputStream writer = new FileOutputStream(des, true);   
		long startTime = System.currentTimeMillis();
		
		for(int i = 0;i< fs.size();i++){			
			srcFile = fs.elementAt(i).toString();
			src = new File(srcFile);
			srcSize = src.length();
		
			desSize = des.length();
			location = String.valueOf(desSize) + '*' +String.valueOf(srcSize);
		
			hash = CalHash.calcuMD5(src);
			hashStr = CalHash.calHexString(hash);
			System.out.println(hashStr);
			luid = UUID.randomUUID().toString();
			
			sp.addSrcElement(luid, hashStr);
			
			
			if(sp.getDesValue(hashStr)!=null){
				sp.addDesElement(hashStr, location);
				try {
		            byte[] bytes=FileUtils.readFileToByteArray(src); 
		           
		            writer.write(bytes); 
	
		        } catch (IOException e) {   
		            e.printStackTrace();   
		        }	
			}
		}
        writer.flush();     
        writer.close();   
		sp.closeSrcDB();
		sp.closeDesDB();
		long endTime = System.currentTimeMillis();
		System.out.println(endTime-startTime);
	}
	public static void SISalone() throws NoSuchAlgorithmException, DigestException, IOException{
		Vector fs = new Vector();
		SISProcess sp = new SISProcess();
		sp.chooseFile(fs);	
		String srcDB = "D:/SIS/dbEnv";
		String desDB = "E:/SIS/dbEnv";
		
		sp.createSrcDB(srcDB);
		sp.createDesDB(desDB);
		String srcFile ;
		File src ;
		long srcSize ;
		String desFile ;
		File des ;
		long desSize ;
		String location ;
		byte[] hash ;
		String hashStr ;
		String luid ;
		long startTime = System.currentTimeMillis();
		for(int i = 0;i< fs.size();i++){			
			srcFile = fs.elementAt(i).toString();
			src = new File(srcFile);
			srcSize = src.length();
			luid = UUID.randomUUID().toString();
			location = luid;
		
			hash = CalHash.calcuMD5(src);
			hashStr = CalHash.calHexString(hash);
			System.out.println(hashStr);
			
			
			sp.addSrcElement(luid, hashStr);
			
			
			if(sp.getDesValue(hashStr)==null){
				sp.addDesElement(hashStr, location);
				FileUtil.copyFileToDir("E:/SIS/des2/", src, hashStr);
	        }	
		}
		sp.closeSrcDB();
		sp.closeDesDB();
		long endTime = System.currentTimeMillis();
		System.out.println(endTime-startTime);
	}
	public static void main(String[] args) throws NoSuchAlgorithmException, DigestException, IOException{
//		SISalone();
		restoreFilealone();
	/*	SISProcess sp = new SISProcess();
		String srcDB = "D:/SIS/dbEnv";
		String desDB = "E:/SIS/dbEnv";
		sp.createDB(srcDB);
		sp.cursorDB();
		sp.closeDB();
		sp.createDB(desDB);
		sp.cursorDB();
		sp.closeDB();*/	
}
	public static void restoreFilealone() throws NoSuchAlgorithmException, DigestException, IOException{
		SISProcess sp = new SISProcess();
		String desFolder = "E:/SIS/des/";
//		String srcDB = "D:/SIS/dbEnv";
		String desFile = "E:/SIS/ALL.txt";
		FileInputStream fis = new FileInputStream(desFile);
		byte[] buffer =new byte[(int) new File(desFile).length()];
		fis.read(buffer);
		sp.createSrcDB(srcDB);
		long startTime = System.currentTimeMillis();
		Cursor cursor = null;
		 // Open the cursor. 
		try {
	    cursor = srcDatabase.openCursor(null, null);

	    // Cursors need a pair of DatabaseEntry objects to operate. These hold
	    // the key and data found at any given position in the database.
	    DatabaseEntry foundKey = new DatabaseEntry();
	    DatabaseEntry foundData = new DatabaseEntry();

	    // To iterate, just call getNext() until the last database record has been 
	    // read. All cursor operations return an OperationStatus, so just read 
	    // until we no longer see OperationStatus.SUCCESS
		
	    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
	        OperationStatus.SUCCESS) {
	        // getData() on the DatabaseEntry objects returns the byte array
	        // held by that object. We use this to get a String value. If the
	        // DatabaseEntry held a byte array representation of some other data
	        // type (such as a complex object) then this operation would look 
	        // considerably different.
	        String keyString = new String(foundKey.getData());
	        String dataString = new String(foundData.getData());
	       FileUtil.copyFileToDir("E:/SIS/des3/", new File("E:/SIS/des2/"+dataString), UUID.randomUUID().toString());


	    }
	} catch (DatabaseException de) {
	    System.err.println("Error accessing database." + de);
	} finally {
	    // Cursors must be closed.
	    cursor.close();
	}
		sp.closeSrcDB();
		sp.closeDesDB();
		long endTime = System.currentTimeMillis()-startTime;
		System.out.println(endTime);
	}
	public static void restoreFile() throws NoSuchAlgorithmException, DigestException, IOException{
		SISProcess sp = new SISProcess();
		String desFolder = "E:/SIS/des/";
//		String srcDB = "D:/SIS/dbEnv";
		String desFile = "E:/SIS/ALL.txt";
		FileInputStream fis = new FileInputStream(desFile);
		byte[] buffer =new byte[(int) new File(desFile).length()];
		fis.read(buffer);
		sp.createSrcDB(srcDB);
		sp.createDesDB(desDB);
		long startTime = System.currentTimeMillis();
		Cursor cursor = null;
		 // Open the cursor. 
		try {
	    cursor = srcDatabase.openCursor(null, null);

	    // Cursors need a pair of DatabaseEntry objects to operate. These hold
	    // the key and data found at any given position in the database.
	    DatabaseEntry foundKey = new DatabaseEntry();
	    DatabaseEntry foundData = new DatabaseEntry();

	    // To iterate, just call getNext() until the last database record has been 
	    // read. All cursor operations return an OperationStatus, so just read 
	    // until we no longer see OperationStatus.SUCCESS
		
	    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
	        OperationStatus.SUCCESS) {
	        // getData() on the DatabaseEntry objects returns the byte array
	        // held by that object. We use this to get a String value. If the
	        // DatabaseEntry held a byte array representation of some other data
	        // type (such as a complex object) then this operation would look 
	        // considerably different.
	        String keyString = new String(foundKey.getData());
	        String dataString = new String(foundData.getData());
	        
	        String[] size = getDesValue(dataString).split("[*]");
	        int l1 = Integer.valueOf(size[0]);
	        int l2 = Integer.valueOf(size[1]);

//	        System.out.println("Key - Data : " + keyString + " - " + dataString + "");
			FileOutputStream fos = new FileOutputStream(desFolder+keyString);
			fos.write(buffer, l1, l2);

	    }
	} catch (DatabaseException de) {
	    System.err.println("Error accessing database." + de);
	} finally {
	    // Cursors must be closed.
	    cursor.close();
	}
		sp.closeSrcDB();
		sp.closeDesDB();
		long endTime = System.currentTimeMillis()-startTime;
		System.out.println(endTime);
	}
	
}