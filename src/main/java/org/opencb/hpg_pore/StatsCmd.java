package org.opencb.hpg_pore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.hpg_pore.commandline.StatsCommandLine;
import org.opencb.hpg_pore.hadoop.HadoopStatsCmd;
import org.opencb.hpg_pore.hadoop.StatsWritable;


import com.beust.jcommander.JCommander;

public class StatsCmd {

	//-----------------------------------------------------------------------//
	// 	                     S T A T S     C O M M A N D                     //
	//-----------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		StatsCommandLine cmdLine = new StatsCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " stats");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			
			cmd.usage();
			System.exit(-1);
		}

		if (cmdLine.isHadoop()) {
			runHadoopStatsCmd(cmdLine.getIn(), cmdLine.getOut(), cmdLine.getlib());
		} else {
			runLocalStatsCmd(cmdLine.getIn(), cmdLine.getOut(), cmdLine.getlib());
		}		
	}

	//-----------------------------------------------------------------------//
	//  local stats command                                                  //
	//-----------------------------------------------------------------------//


	private static void runLocalStatsCmd(String in, String out, String lib) {	
		File inFile = new File(in);
		if (!inFile.exists()) {
			System.out.println("Error: Local directory " + in + " does not exist!");
			System.exit(-1);						
		}

		NativePoreSupport.loadLibrary(lib);

		// initialize PrintWriter map
		HashMap<String, StatsWritable> statsMap = new HashMap<String, StatsWritable>();

		// process file depending on File or Folder
		if (inFile.isDirectory()) {
			processLocalDir(inFile, statsMap);
		} else if (inFile.isFile()) {
			processLocalFile(inFile, statsMap);
		}

		// print results and charts
		printResults(statsMap, out);
		// Draw the grafics
		drawResults(statsMap,out);
		
	}

	//-----------------------------------------------------------------------//

	private static void printResults(HashMap<String, StatsWritable> statsMap, String outDir) {
		
		String summaryFileName = outDir + "/summary.txt";
		
		try {
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(summaryFileName, false)));
			for(String key: statsMap.keySet()) {
				writer.print(Utils.createSummaryFile(statsMap.get(key), key));
			}
			writer.close();		

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error writing statitics results to the output folder :" + outDir);
		}
		
	}

	//-----------------------------------------------------------------------//

	private static void processLocalFile(File inFile, HashMap<String, StatsWritable> statsMap) {

		byte[] content = Utils.read(inFile);
		String info = new NativePoreSupport().getInfo(content);		
		if (info == null) {
			System.out.println("Error reading file " + inFile.getAbsolutePath() + ". Maybe, the file is corrupt.");
			return;
		}
		//System.out.println("getInfo:");
		//System.out.println(info);
		
		StatsWritable stats = new StatsWritable();
		
		String runId = Utils.getValue("run_id", info);
		
		String fastqs = new NativePoreSupport().getFastqs(content);
		Utils.parseAndInitStats(info, fastqs, stats);
		
		
		if (!statsMap.containsKey(runId)) {
			statsMap.put(runId, stats);
		} else {
			statsMap.get(runId).update(stats);
			//System.out.println(stats.toFormat());
		}
		//System.out.println(stats.toFormat());
		
		
	}

	//-----------------------------------------------------------------------//

	private static void processLocalDir(File inDir, HashMap<String, StatsWritable> statsMap) {
		for (final File fileEntry : inDir.listFiles()) {
			if (fileEntry.isDirectory()) {
				processLocalDir(fileEntry, statsMap);
			} else {
				processLocalFile(fileEntry, statsMap);
			}
		}
	}

	
	//-----------------------------------------------------------------------//
	
	private static void drawResults(HashMap<String, StatsWritable> statsMap, String outDir) {
		try {
			
			for(String key: statsMap.keySet()) {
				(statsMap.get(key)).draw(key, outDir);	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error drawing statitics results to the output folder :" + outDir);
		}
		
		
	}
	//-----------------------------------------------------------------------//
	//  hadoop stats command                                                 //
	//-----------------------------------------------------------------------//

	private static void runHadoopStatsCmd(String in, String out, String lib) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(new Path(in))) {
			System.out.println("Error: Hdfs file " + in + " does not exist!");
			System.exit(-1);			
		}

		String outHdfsDirname = new String(in + "-" + new Date().getTime());
		System.out.println(in + ", " + out + ", " + outHdfsDirname);

		String[] args = new String[3];
		args[0] = new String(in);
		args[1] = new String(outHdfsDirname);
		args[2] = new String(lib);

		// map-reduce
		int error = ToolRunner.run(new HadoopStatsCmd(), args);
		if (error != 0) {
			System.out.println("Error: Running map-reduce job!");
			System.exit(-1);			
		}

		// post-processing
		Path outFile = new Path(outHdfsDirname + "/part-r-00000");
		System.out.println("out file = " + outFile.getName());

		if (!fs.exists(outFile)) {
			System.out.println("out file = " + outFile.getName() + " does not exist !!");
		} else {
			String outRawFileName = out + "/raw.txt";
			fs.copyToLocalFile(outFile, new Path(outRawFileName));
			
			Utils.parseStatsFile(outRawFileName, out);
		}
		fs.delete(new Path(outHdfsDirname), true);		
	}

	//-----------------------------------------------------------------------//
	//-----------------------------------------------------------------------//
}
