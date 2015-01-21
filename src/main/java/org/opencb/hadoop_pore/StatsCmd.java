package org.opencb.hadoop_pore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;

import org.opencb.hadoop_pore.commandline.StatsCommandLine;
import org.opencb.hadoop_pore.hadoop.StatsWritable;

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
			runHadoopStatsCmd(cmdLine.getIn(), cmdLine.getOut());
		} else {
			runLocalStatsCmd(cmdLine.getIn(), cmdLine.getOut());
		}		
	}

	//-----------------------------------------------------------------------//
	//  local stats command                                                  //
	//-----------------------------------------------------------------------//


	private static void runLocalStatsCmd(String in, String out) {	
		File inFile = new File(in);
		if (!inFile.exists()) {
			System.out.println("Error: Local directory " + in + " does not exist!");
			System.exit(-1);						
		}

		NativePoreSupport.loadLibrary();

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
	}

	//-----------------------------------------------------------------------//

	private static void printResults(HashMap<String, StatsWritable> statsMap, String outDir) {
		String rawFileName = outDir + "/raw.txt";

		try {
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(rawFileName, false)));
			for(String key: statsMap.keySet()) {
				writer.print(key);
				writer.print(statsMap.get(key).toFormat());
			}
			writer.close();		

			Utils.parseStatsFile(rawFileName, outDir);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error writing statitics results to the output folder :" + outDir);
		}
	}

	//-----------------------------------------------------------------------//

	private static void processLocalFile(File inFile, HashMap<String, StatsWritable> statsMap) {

		String info = new NativePoreSupport().getInfo(Utils.read(inFile));		

		StatsWritable stats = new StatsWritable();
		String runId = stats.parseAndInit(info);

		if (!statsMap.containsKey(runId)) {
			statsMap.put(runId, stats);
		} else {
			statsMap.get(runId).update(stats);
		}
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
	//  hadoop stats command                                                 //
	//-----------------------------------------------------------------------//

	private static void runHadoopStatsCmd(String in, String out) throws Exception {
	}

	//-----------------------------------------------------------------------//
	//-----------------------------------------------------------------------//
}
