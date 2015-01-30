package org.opencb.hpg_pore.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.opencb.hpg_pore.Main;
import org.opencb.hpg_pore.Utils;
import org.opencb.hpg_pore.commandline.ImportCommandLine;

import com.beust.jcommander.JCommander;

public class HadoopImportCmd {
	
	//-----------------------------------------------------------------------//
	// 	                     I M P O R T     C O M M A N D                   //
	//-----------------------------------------------------------------------//
	
	public static void run(String[] args) {	
		ImportCommandLine cmdLine = new ImportCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " import");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}

		String srcDirName = cmdLine.getIn();
		String destFileName = cmdLine.getOut();
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Text key = null;
		BytesWritable value = null;
		
		MapFile.Writer writer = null;
		//SequenceFile.Writer writer = null;
		try {
			if (cmdLine.isCompression()) {
				//writer = SequenceFile.createWriter(fs, conf, new Path(destFileName), Text.class, BytesWritable.class, CompressionType.BLOCK);
				writer = new MapFile.Writer(conf, fs, destFileName, Text.class, BytesWritable.class, CompressionType.BLOCK);
			} else {
				//writer = SequenceFile.createWriter(fs, conf, new Path(destFileName), Text.class, BytesWritable.class, CompressionType.NONE);				
				writer = new MapFile.Writer(conf, fs, destFileName, Text.class, BytesWritable.class);
			}
		} catch (IOException e) {
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
			content = Utils.read(fast5);
			if (content != null) {
				key = new Text(fast5.getName());
				value = new BytesWritable(content);
				System.out.println("key = " + key);
				try {
					writer.append(key, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		IOUtils.closeStream(writer);
	}
	
	//-----------------------------------------------------------------------//
}
