package org.opencb.hpg_pore;


import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.opencb.hpg_pore.commandline.Fast5NamesCommandLine;

import com.beust.jcommander.JCommander;

public class Fast5NamesCmd {
	
	//---------------------------------------------------------------------------------//
	// 	                     F A S T 5_N A M E S     C O M M A N D                     //
	//---------------------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		Fast5NamesCommandLine cmdLine = new Fast5NamesCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + "fast5names");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}

		
		runHadoopGetNames(cmdLine.getin(), cmdLine.getOut());
		
	}

	private static void runHadoopGetNames(String in, String out) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(new Path(in))) {
			System.out.println("Error: Hdfs file " + in + " does not exist!");
			System.exit(-1);			
		}

		NativePoreSupport.loadLibrary();

		MapFile.Reader reader = null;
		PrintWriter writer = null;
				  
		try {
			
			writer = new PrintWriter(out + "/Fast5Filenames.txt", "UTF-8");
			try {
				reader = new MapFile.Reader(fs, in, conf);
				Text key = (Text) reader.getKeyClass().newInstance();
				BytesWritable value = (BytesWritable) reader.getValueClass().newInstance();			
				while (reader.next(key, value)){
				
					writer.println(key.toString() + "\n");
				}
			} catch (IOException | InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
 
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
                if(reader != null)
				reader.close();
                writer.close();
  		}
		
	
	}
	
}
