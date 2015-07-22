package org.opencb.hpg_pore;


import java.io.File;

import java.io.IOException;

import java.text.NumberFormat;
import java.util.HashMap;


import org.jfree.chart.JFreeChart;
import org.opencb.hpg_pore.commandline.SignalCommandLine;


import com.beust.jcommander.JCommander;

public class SignalCmd {

	public static String outDir;
	//-----------------------------------------------------------------------//
	// 	                    S Q U I G G L E    C O M M A N D                 //
	//-----------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		SignalCommandLine cmdLine = new SignalCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " signal");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}
		
		if (cmdLine.isHadoop()) {
			runHadoopSignalCmd(cmdLine.getIn(), cmdLine.getFast5name(), cmdLine.getOut(), cmdLine.getMin(), cmdLine.getMax());
		} else {
			runLocalSignalCmd(cmdLine.getIn(), cmdLine.getOut(), cmdLine.getMin(), cmdLine.getMax());
		}		
	}

	//-----------------------------------------------------------------------//
	//  local squiggle command                                               //
	//-----------------------------------------------------------------------//


	private static void runLocalSignalCmd(String in, String out, int min, int max) throws IOException {
		File inFile = new File(in);
		if (!inFile.exists()) {
			System.out.println("Error: Local directory " + in + " does not exist!");
			System.exit(-1);						
		}

		NativePoreSupport.loadLibrary();
		int width = 1024;
		int height = 480;

		NumberFormat nf = NumberFormat.getInstance();

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
				//map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
				try {
					map.put(nf.parse(linea[1]).doubleValue(), nf.parse(linea[0]).doubleValue());
				} catch (Exception e) {
				}
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
				//map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
				try {
					map.put(nf.parse(linea[1]).doubleValue(), nf.parse(linea[0]).doubleValue());
				} catch (Exception e) {
				}
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
				//map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
				try {
					map.put(nf.parse(linea[1]).doubleValue(), nf.parse(linea[0]).doubleValue());
				} catch (Exception e) {
				}
			}
	
			JFreeChart chart = Utils.plotSignalChart(map, "Signal for 2D", "measured signal", "time");
			Utils.saveChart(chart, width, height, out + "/2D_signal.jpg");
		}else{
			
			System.out.println("There is no 2D event type");
		}
	}

	
	
	//-----------------------------------------------------------------------//
	//  hadoop squiggle command                                              //
	//-----------------------------------------------------------------------//

	private static void runHadoopSignalCmd(String in, String nameFile, String out, int min, int max) throws Exception {
				
		NativePoreSupport.loadLibrary();
		int width = 1024;
		int height = 480;

		NumberFormat nf = NumberFormat.getInstance();

		/*******************************
		// T E M P L A T E
		 *****************************/
		String events = null;
		events = new NativePoreSupport().getEvents(Utils.readHadoop(in, nameFile), "template", min, max);
		
		if(events != null){
			//parsear la señal
			HashMap<Double, Double> map = new HashMap<Double, Double>();
			String[] linea;
			String[] lineas = events.split("\n");
			for (int i = 1 ; i< lineas.length; i++){
				linea = lineas[i].split("\t");
				//map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
				try {
					map.put(nf.parse(linea[1]).doubleValue(), nf.parse(linea[0]).doubleValue());
				} catch (Exception e) {
				}
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
		events = new NativePoreSupport().getEvents(Utils.readHadoop(in, nameFile), "complement", min, max);
		
		if(events!= null){
			//parsear la señal
			HashMap<Double, Double> map = new HashMap<Double, Double>();
			String[] linea;
			String[]lineas = events.split("\n");
			for (int i = 1 ; i< lineas.length; i++){

				linea = lineas[i].split("\t");
				//map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
				try {
					map.put(nf.parse(linea[1]).doubleValue(), nf.parse(linea[0]).doubleValue());
				} catch (Exception e) {
				}
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
		events = new NativePoreSupport().getEvents(Utils.readHadoop(in, nameFile), "2D", min, max);
		if(events !=null){
			//parsear la señal
			HashMap<Double, Double> map = new HashMap<Double, Double>();
			String[] linea;
			String[] lineas = events.split("\n");
			for (int i = 1 ; i< lineas.length; i++){
			
				linea = lineas[i].split("\t");
				//map.put(Double.parseDouble(linea[1]),Double.parseDouble(linea[0]));
				try {
					map.put(nf.parse(linea[1]).doubleValue(), nf.parse(linea[0]).doubleValue());
				} catch (Exception e) {
				}
			}
	
			JFreeChart chart = Utils.plotSignalChart(map, "Signal for 2D", "measured signal", "time");
			Utils.saveChart(chart, width, height, out + "/2D_signal.jpg");
		}else{
			
			System.out.println("There is no 2D event type");
		}
	}

	//-----------------------------------------------------------------------//
	//-----------------------------------------------------------------------//
}
