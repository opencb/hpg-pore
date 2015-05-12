package org.opencb.hpg_pore;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.opencb.hpg_pore.commandline.ExportCommandLine;

import com.beust.jcommander.JCommander;

public class ExportCmd {
	public static String outDir;
	//--------------------------------------------------------------------------//
	// 	                     E X P O R T     C O M M A N D                     //
	//-------------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		ExportCommandLine cmdLine = new ExportCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " export");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}
		outDir = cmdLine.getOut();
		if (cmdLine.getFast5name() == null) {
			runHadoopGetFiles(cmdLine.getIn());
		} else {
			runHadoopGetFile(cmdLine.getIn(), cmdLine.getFast5name());
		}
		
	}
	
	private static void runHadoopGetFiles(String in) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(new Path(in))) {
			System.out.println("Error: Hdfs file " + in + " does not exist!");
			System.exit(-1);			
		}

		NativePoreSupport.loadLibrary();

		MapFile.Reader reader = null;

			try {
				reader = new MapFile.Reader(fs, in, conf);
				Text key = (Text) reader.getKeyClass().newInstance();
				BytesWritable value = (BytesWritable) reader.getValueClass().newInstance();			
				while (reader.next(key, value)){
					createFile(key.toString(),value);
				}
			} catch (IOException | InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
 
		
		} finally {
                if(reader != null)
				reader.close();
               
  		}
		
	
	}
	private static void runHadoopGetFile(String in, String nameFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(new Path(in))) {
			System.out.println("Error: Hdfs file " + in + " does not exist!");
			System.exit(-1);			
		}

		NativePoreSupport.loadLibrary();

		MapFile.Reader reader = null;

			try {
				reader = new MapFile.Reader(fs, in, conf);
				Text key = new Text( nameFile);
				BytesWritable value = (BytesWritable) reader.getValueClass().newInstance();			
				if( reader.seek(key)){ // if the key exist
					reader.get(key, value);
					createFile(key.toString(),value);
				}else{
					System.out.println("Error: Hdfs file " + nameFile + " not exist! check this!");
					System.exit(-1);
				}
			} catch (IOException | InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
 
		
		} finally {
                if(reader != null)
				reader.close();
               
  		}
	}
	
	private static void createFile(String key, BytesWritable b) throws IOException {
		/*PrintWriter writer = null;
		writer = new PrintWriter(outDir + "/"+ key, "UTF-8");
		writer.append(b.toString());
		writer.close();*/
		//ObjectOutputStream file = new ObjectOutputStream(new FileOutputStream( outDir + "/"+ key ));
		FileOutputStream file = new FileOutputStream(outDir + "/" +  key);
		try{
			System.out.println("name = " + key + ", length = " + b.getLength() + ", capacity = " + b.getCapacity() + ", bytes.length = " + b.getBytes().length + ", mark: " + ((char) b.getBytes()[0]) + ((char) b.getBytes()[1]) + ((char) b.getBytes()[2]) + ((char) b.getBytes()[3]));
			file.write(b.getBytes(), 0, b.getLength());
		}catch (IOException e){
			e.printStackTrace();
		}finally {
	        
	        file.close();
		}
	}
	
}
