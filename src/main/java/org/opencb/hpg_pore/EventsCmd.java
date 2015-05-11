package org.opencb.hpg_pore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.hpg_pore.commandline.EventsCommandLine;
import org.opencb.hpg_pore.hadoop.HadoopFastqCmd;

import com.beust.jcommander.JCommander;

public class EventsCmd {
	
	public static String outDir;
	//-----------------------------------------------------------------------//
	// 	                    E V E N T S    C O M M A N D                     //
	//-----------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		EventsCommandLine cmdLine = new EventsCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " Events");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}

		if (cmdLine.isHadoop()) {
			runHadoopEventsCmd(cmdLine.getin(), cmdLine.getOut(), cmdLine.getmin(), cmdLine.getmax());
		} else {
			runLocalEventsCmd(cmdLine.getin(), cmdLine.getOut(), cmdLine.getmin(), cmdLine.getmax());
		}		
	}

	//-----------------------------------------------------------------------//
	//  local Events command                                                 //
	//-----------------------------------------------------------------------//
	
	private static void runLocalEventsCmd(String in, String out, int min, int max) throws IOException {
		File inFile = new File(in);
		if (!inFile.exists()) {
			System.out.println("Error: Local directory " + in + " does not exist!");
			System.exit(-1);						
		}

		NativePoreSupport.loadLibrary();
		outDir = out;
		
		/*******************************
		// T E M P L A T E
		 *****************************/
		
		String events = null;
		events = new NativePoreSupport().getEvents(Utils.read(inFile), "template", min, max);
		if(events != null){
			//parsear la señal
			String sFichero = outDir +"/template_Events.txt";
		
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(sFichero, false)));
			String[] linea;
			String[] lineas = events.split("\n");
			
			for (int i = 0 ; i< lineas.length; i++){
				linea = lineas[i].split("\t");
				for (int j = 0; j<linea.length;j++){
					writer.print(linea[j] + "\t");
				}
				writer.print("\n");
			}
			writer.close();	
			
		}else{
			
			System.out.println("There is no template event type");
		}
		
		/**********************************
		// C O M P L E M E N T
		 **********************************/
		events = null;
		events = new NativePoreSupport().getEvents(Utils.read(inFile), "complement", min, max);
		
		if(events != null){
			//parsear la señal
			String sFichero = outDir +"/complement_Events.txt";
		
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(sFichero, false)));
			String[] linea;
			String[] lineas = events.split("\n");
			
			for (int i = 0 ; i< lineas.length; i++){
				linea = lineas[i].split("\t");
				for (int j = 0; j<linea.length;j++){
					writer.print(linea[j] + "\t");
				}
				writer.print("\n");
			}
			writer.close();
			
		}else{
			
			System.out.println("There is no complement event type");
		}
		
		/***********************************
		// 2 D
		*************************************/
		events = null;
		events = new NativePoreSupport().getEvents(Utils.read(inFile), "2D", min, max);
		if(events != null){
			//parsear la señal
			
			//parsear la señal
			String sFichero = outDir +"/2D_Events.txt";
		
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(sFichero, false)));
			String[] linea;
			String[] lineas = events.split("\n");
			
			for (int i = 0 ; i< lineas.length; i++){
				linea = lineas[i].split("\t");
				for (int j = 0; j<linea.length;j++){
					writer.print(linea[j] + "\t");
				}
				writer.print("\n");
			}
			writer.close();
				
			
		}else{
			
			System.out.println("There is no 2D event type");
		}
	
		
	}
	

	//-----------------------------------------------------------------------//
	//  hadoop Events command                                              //
	//-----------------------------------------------------------------------//

	private static void runHadoopEventsCmd(String in, String out, int min, int max) throws Exception {
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
		fs.delete(new Path(outHdfsDirname), true);
	}

	//-----------------------------------------------------------------------//
	//-----------------------------------------------------------------------//
}
