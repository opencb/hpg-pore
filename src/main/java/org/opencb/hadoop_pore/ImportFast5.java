package org.opencb.hadoop_pore;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class ImportFast5 {
	
	//-----------------------------------------------------------------------//
	// 	                     I M P O R T     C O M M A N D                   //
	//-----------------------------------------------------------------------//
	
	public static void run(String[] args) {	
		if (args.length != 3) {
			System.out.println("Error: Mismatch parameters for import-fast5 command");
			importHelp();
			System.exit(0);
		}
				
		String srcDirName = args[1];
		String destFileName = args[2];
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Text key = null;
		BytesWritable value = null;
		
		//MapFile.Writer writer = null;
		SequenceFile.Writer writer = null;
		try {
			//writer = new MapFile.Writer(conf, fs, destFileName, Text.class, BytesWritable.class);
			writer = SequenceFile.createWriter(fs, conf, new Path(destFileName), Text.class, BytesWritable.class, CompressionType.NONE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		byte[] content;
		File srcDir = new File(srcDirName);
		
		List<String> names = new ArrayList<String>();
		for (Object obj : FileUtils.listFiles(srcDir, null, false)) {
			names.add(((File) obj).getAbsolutePath()); 
		}
		Collections.sort(names);
		
		File fast5;
		for (String name: names) {
			fast5 = new File(name);
			content = read(fast5);
			if (content != null) {
				key = new Text(fast5.getName());
				value = new BytesWritable(content);
				System.out.println("key = " + key);
				try {
					writer.append(key, value);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		IOUtils.closeStream(writer);
	}
	
	//-----------------------------------------------------------------------//

	public static void importHelp() {
		System.out.println("import-fast5 command:");
		System.out.println("\thadoop jar hadoop-nano.jar import-fast5 <source> <destination>");
		System.out.println("Options:");
		System.out.println("\tsource     : local folder of fast5 files");
		System.out.println("\tdestination: destination hdfs file");
	}
	
	//-----------------------------------------------------------------------//
	// Read the given binary file, and return its contents as a byte array   //
	//-----------------------------------------------------------------------//
	static byte[] read(File file){
		System.out.println("Reading in binary file named : " + file.getAbsolutePath());
		System.out.println("File size: " + file.length());
		byte[] result = new byte[(int)file.length()];
		try {
			InputStream input = null;
			try {
				int totalBytesRead = 0;
				input = new BufferedInputStream(new FileInputStream(file));
				while(totalBytesRead < result.length){
					int bytesRemaining = result.length - totalBytesRead;
					//input.read() returns -1, 0, or more :
					int bytesRead = input.read(result, totalBytesRead, bytesRemaining); 
					if (bytesRead > 0){
						totalBytesRead = totalBytesRead + bytesRead;
					}
				}
				// the above style is a bit tricky: it places bytes into the 'result' array; 
				// 'result' is an output parameter;
				// the while loop usually has a single iteration only.
				System.out.println("Num bytes read: " + totalBytesRead);
			}
			finally {
				System.out.println("Error: Closing input stream.");
				input.close();
			}
		}
		catch (FileNotFoundException ex) {
			System.out.println("Error: File not found.");
		}
		catch (IOException ex) {
			System.out.println(ex);
		}
		return result;
	}

}
