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
		cmd.setProgramName(Main.BINARY_NAME + " events");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}

		if (cmdLine.isHadoop()) {
			runHadoopEventsCmd(cmdLine.getIn(), cmdLine.getFast5name(), cmdLine.getOut(), cmdLine.getMin(), cmdLine.getMax());
		} else {
			runLocalEventsCmd(cmdLine.getIn(), cmdLine.getOut(), cmdLine.getMin(), cmdLine.getMax());
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
			String sFichero = outDir +"/template_events.txt";
		
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
			String sFichero = outDir +"/complement_events.txt";
		
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
			String sFichero = outDir +"/2D_events.txt";
		
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

	private static void runHadoopEventsCmd(String in, String fast5name, String out, int min, int max) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(new Path(in))) {
			System.out.println("Error: Hdfs file " + in + " does not exist!");
			System.exit(-1);
		}

		NativePoreSupport.loadLibrary();

		String mapFilename = in;

		byte content[];
		String events = null;

		String types[] = {"template", "complement", "2D"};
		String namefiles[] = {"/template_events.txt", "/complement_events.txt","/2D_events.txt"};

		for (int i  = 0; i <3 ; i++ ) {
			System.out.println("\n" + types[i] + ", reading " + in + ".....:\n");
			content = Utils.readHadoop(mapFilename, fast5name);
			if (content != null) {
				events = new NativePoreSupport().getEvents(content, types[i], min, max);
				if(events != null){

					String sFichero = out +namefiles[i];
					PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(sFichero, false)));
					String[] linea;
					String[] lineas = events.split("\n");

					for (int w = 0 ; w< lineas.length; w++){
						linea = lineas[w].split("\t");
						for (int j = 0; j<linea.length;j++){
							writer.print(linea[j] + "\t");
						}
						writer.print("\n");
					}
					writer.close();
				}

			} else {
				System.out.println(types[i] +  " is empty !!! ");
			}
		}
	}

	//-----------------------------------------------------------------------//
	//-----------------------------------------------------------------------//
}
