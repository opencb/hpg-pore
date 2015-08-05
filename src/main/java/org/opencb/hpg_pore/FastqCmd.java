package org.opencb.hpg_pore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.hpg_pore.commandline.FastqCommandLine;
import org.opencb.hpg_pore.hadoop.HadoopFastqCmd;

import com.beust.jcommander.JCommander;

public class FastqCmd {

	public static String outDir;
	//-----------------------------------------------------------------------//
	// 	                     F A S T Q     C O M M A N D                     //
	//-----------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		FastqCommandLine cmdLine = new FastqCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " fastq");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}

		if (cmdLine.isHadoop()) {
			runHadoopFastqCmd(cmdLine.getIn(), cmdLine.getOut());
		} else {
			runLocalFastqCmd(cmdLine.getIn(), cmdLine.getOut());
		}		
	}

	//-----------------------------------------------------------------------//
	//  local fastq command                                                  //
	//-----------------------------------------------------------------------//


	private static void runLocalFastqCmd(String in, String out) {
		File inFile = new File(in);
		if (!inFile.exists()) {
			System.out.println("Error: Local directory " + in + " does not exist!");
			System.exit(-1);						
		}

		outDir = out;

		NativePoreSupport.loadLibrary();

		// initialize PrintWriter map
		HashMap<String, PrintWriter> printers = new HashMap<String, PrintWriter>();

		// process file depending on File or Folder
		if (inFile.isDirectory()) {
			processLocalDir(inFile, printers);
		} else if (inFile.isFile()) {
			processLocalFile(inFile, printers);
		}

		// close printers
		for (String name: printers.keySet()) {
			printers.get(name).close();
		}
	}

	//-----------------------------------------------------------------------//

	private static void writeToLocalFile(String name, String content, HashMap<String, PrintWriter> writers) throws IOException {

		PrintWriter writer = null;
		if (!writers.containsKey(name)) {
			String[] fields = name.split("-");
			File auxFile = new File(outDir + "/" + fields[0]);
			if (!auxFile.exists()) {
				auxFile.mkdir();
			}

			auxFile = new File(auxFile.getAbsolutePath() + "/" + Utils.toModeString(fields[1]) + ".fq");
			writer = new PrintWriter(new BufferedWriter(new FileWriter(auxFile.getAbsolutePath(), false))); //true)));

			writers.put(name, writer);
		}
		writer = writers.get(name);
		writer.print(content);		
	}

	//-----------------------------------------------------------------------//

	private static void processLocalFile(File inFile, HashMap<String, PrintWriter> printers) {

		String fastqs = null;
		fastqs = new NativePoreSupport().getFastqs(Utils.read(inFile));		
		//System.out.println(fastqs);

		String name, line, content;
		String[] lines;

		try {
			lines = fastqs.split("\n");
		} catch (Exception e) {
			System.out.println("Error reading FastQ sequences from file " + inFile.getAbsolutePath() + ". Skip.");
			return;
		}

		for (int i = 0; i < lines.length; i += 5) {
			// first line: runId & template/complement/2d				
			line = lines[i];
			System.out.println(i + " of " + lines.length + " : " + line);
			name = new String(line);

			if (i + 1 >= lines.length) break;

			// second line: read ID
			line = lines[i + 1];
			content = new String(line + "\n");

			// third line: nucleotides
			line = lines[i + 2];
			content = content.concat(line).concat("\n");

			// third line: nucleotides
			line = lines[i + 3];
			content = content.concat(line).concat("\n");

			// third line: nucleotides
			line = lines[i + 4];
			content = content.concat(line).concat("\n");

			// write to file
			try {
				writeToLocalFile(name, content, printers);
			} catch (Exception e) {
				System.out.println("Error writing fasta sequences from " + inFile.getAbsolutePath());
			}
			//multipleOutputs.write(NullWritable.get(), content, name);
		}
	}

	//-----------------------------------------------------------------------//

	private static void processLocalDir(File inDir, HashMap<String, PrintWriter> printers) {
		for (final File fileEntry : inDir.listFiles()) {
			if (fileEntry.isDirectory()) {
				processLocalDir(fileEntry, printers);
				//listFilesForFolder(fileEntry);
			} else {
				processLocalFile(fileEntry, printers);
			}
		}
	}

	//-----------------------------------------------------------------------//
	//  hadoop fastq command                                                 //
	//-----------------------------------------------------------------------//

	private static void runHadoopFastqCmd(String in, String out) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		LocalFileSystem localFileSystem = fs.getLocal(conf);

		if (!fs.exists(new Path(in))) {
			System.out.println("Error: Hdfs file " + in + " does not exist!");
			System.exit(-1);			
		}

		NativePoreSupport.checkLibrary(fs);

		String outHdfsDirname = new String(in + "-" + new Date().getTime());
		System.out.println(in + ", " + out + ", " + outHdfsDirname);

		String[] args = new String[2];
		args[0] = new String(in);
		args[1] = new String(outHdfsDirname);

		// map-reduce
		int error = ToolRunner.run(new HadoopFastqCmd(), args);
		if (error != 0) {
			System.out.println("Error: Running map-reduce job!");
			System.exit(-1);			
		}

		// post-processing
		String runId, mode, outLocalRunIdDirname;

		String[] fields;
		FileStatus[] status = fs.listStatus(new Path(outHdfsDirname));
		for (int i=0; i<status.length; i++) {
			fields = status[i].getPath().getName().split("-");
			System.out.println("filename = " + status[i].getPath().getName());
			if (fields.length < 2) continue;

			mode = fields[1];
			if (mode.equalsIgnoreCase("te") ||
					mode.equalsIgnoreCase("co") ||
					mode.equalsIgnoreCase("2D")) {

				System.out.println("processing....");
				runId = fields[0];

				outLocalRunIdDirname = new String(out + "/" + runId);
				File outDir = new File(outLocalRunIdDirname);
				if (!outDir.exists()) {
					outDir.mkdir();
				}


				Path inputPath = new Path(outHdfsDirname + "/" + runId + "-" + mode);
				File outFile = new File(outLocalRunIdDirname + "/" + Utils.toModeString(mode) + ".fq");

				try {
					if (outFile.exists()) {
						outFile.delete();
					}
					System.out.println("Copying " + Utils.toModeString(mode) + " sequences for run " + runId + " to the local file " + outFile.getPath());
					FileUtil.copyMerge(fs, inputPath, localFileSystem, new Path(outFile.getPath()), true, conf, "");
				} catch (IOException ioe) {
					System.out.println (ioe);
				}

			}
		}


		System.out.println("Done");
		fs.delete(new Path(outHdfsDirname), true);

	}



	//-----------------------------------------------------------------------//
	//-----------------------------------------------------------------------//
}

