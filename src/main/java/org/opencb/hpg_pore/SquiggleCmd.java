package org.opencb.hpg_pore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.jfree.chart.JFreeChart;
import org.opencb.hpg_pore.commandline.SquiggleCommandLine;
import org.opencb.hpg_pore.hadoop.HadoopFastqCmd;

import com.beust.jcommander.JCommander;

public class SquiggleCmd {

	public static String outDir;
	//-----------------------------------------------------------------------//
	// 	                    S Q U I G G L E    C O M M A N D                 //
	//-----------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		SquiggleCommandLine cmdLine = new SquiggleCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " squiggle");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}
		
		if (cmdLine.isHadoop()) {
			runHadoopSquiggleCmd(cmdLine.getSrc(), cmdLine.getOut(), cmdLine.getlib(), cmdLine.getmin() , cmdLine.getmax());
		} else {
			//runLocalSquiggleCmd(cmdLine.getSrc(), cmdLine.getOut(), 10, 250);
			runLocalSquiggleCmd(cmdLine.getSrc(), cmdLine.getOut(), cmdLine.getlib(), cmdLine.getmin(), cmdLine.getmax());
		}		
	}

	//-----------------------------------------------------------------------//
	//  local squiggle command                                               //
	//-----------------------------------------------------------------------//


	private static void runLocalSquiggleCmd(String in, String out, String lib, int min, int max) throws IOException {	
		File inFile = new File(in);
		if (!inFile.exists()) {
			System.out.println("Error: Local directory " + in + " does not exist!");
			System.exit(-1);						
		}

		NativePoreSupport.loadLibrary(lib);
		int width = 1024;
		int height = 480;
		
		/*******************************
		// T E M P L A T E
		 *****************************/
		String events = null;
		events = new NativePoreSupport().getEvents(Utils.read(inFile), "template", min, max);
		//System.out.println(events);
		if(events != null){
			//parsear la señal
			HashMap<Double, Double> map = new HashMap<Double, Double>();
			String[] linea;
			String[] lineas = events.split("\n");
			//read the first and get the start time 
			//linea = lineas[1].split("\t");
			//map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
			//double starttime = Double.parseDouble(linea[0]);
			for (int i = 1 ; i< lineas.length; i++){
			//for (int i = 1 ; i< 200; i++){
				//System.out.println("linea numero " + i);
				linea = lineas[i].split("\t");
				//System.out.println("linea[1]: "+ linea[1]+ "   linea[0]: "+linea[0]);
				//if((Double.parseDouble(linea[0])- starttime) < max )
				map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
				//else{
					
				//}
			}
		
			JFreeChart chart = Utils.plotSignalChart(map, "Signal for template", "measured signal", "time");
			Utils.saveChart(chart, width, height, out + "/template_signal.jpg");
		}else{
			
			System.out.println("There is no template event type");
		}
		
		/**********************************
		// C O M P L E M E N T
		 **********************************/
		events = null;
		events = new NativePoreSupport().getEvents(Utils.read(inFile), "complement", min, max);
		
		if(events!= null){
			//parsear la señal
			HashMap<Double, Double> map = new HashMap<Double, Double>();
			String[] linea;
			String[]lineas = events.split("\n");
			for (int i = 1 ; i< lineas.length; i++){
			//for (int i = 1 ; i< 200; i++){
				linea = lineas[i].split("\t");
				map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
			}
			JFreeChart chart = Utils.plotSignalChart(map, "Signal for Complement", "measured signal", "time");
			Utils.saveChart(chart, width, height, out + "/complement_signal.jpg");
		}else{
			
			System.out.println("There is no complement event type");
		}
		
		/***********************************
		// 2 D
		*************************************/
		events = null;
		events = new NativePoreSupport().getEvents(Utils.read(inFile), "2D", min, max);
		if(events !=null){
			//parsear la señal
			HashMap<Double, Double> map = new HashMap<Double, Double>();
			String[] linea;
			String[] lineas = events.split("\n");
			for (int i = 1 ; i< lineas.length; i++){
				//for (int i = 1 ; i< 200; i++){
				linea = lineas[i].split("\t");
				map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
			}
	
			JFreeChart chart = Utils.plotSignalChart(map, "Signal for 2D", "measured signal", "time");
			Utils.saveChart(chart, width, height, out + "/2D_signal.jpg");
		}else{
			
			System.out.println("There is no 2D event type");
		}
	
		
		System.exit(0);

	}

	
	
	//-----------------------------------------------------------------------//
	//  hadoop squiggle command                                              //
	//-----------------------------------------------------------------------//

	private static void runHadoopSquiggleCmd(String in, String out, String lib, int min, int max) throws Exception {
		/*Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(new Path(in))) {
			System.out.println("Error: Hdfs file " + in + " does not exist!");
			System.exit(-1);			
		}

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
			if (fields.length < 2) continue;

			mode = fields[1];
			if (mode.equalsIgnoreCase("te") || 
					mode.equalsIgnoreCase("co") || 
					mode.equalsIgnoreCase("2D")) {
				runId = fields[0];

				outLocalRunIdDirname = new String(out + "/" + runId);
				File outDir = new File(outLocalRunIdDirname);
				if (!outDir.exists()) {
					outDir.mkdir();
				}
				System.out.println("Copying " + Utils.toModeString(mode) + " sequences for run " + runId + " to the local file " + outLocalRunIdDirname + "/" + Utils.toModeString(mode) + ".fq");
				fs.copyToLocalFile(status[i].getPath(), new Path(outLocalRunIdDirname + "/" + Utils.toModeString(mode) + ".fq"));
				System.out.println("Done.");
			}
		}
		fs.delete(new Path(outHdfsDirname), true);*/
	}

	//-----------------------------------------------------------------------//
	//-----------------------------------------------------------------------//
}
