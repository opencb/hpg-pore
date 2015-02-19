package org.opencb.hpg_pore;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.opencb.hpg_pore.hadoop.ParamsforDraw;
import org.opencb.hpg_pore.hadoop.StatsWritable;
import org.opencb.hpg_pore.hadoop.StatsWritable.BasicStats;

public class Utils {

	public static String toModeString(String mode) {
		if (mode.equalsIgnoreCase("te")) {
			return "template";
		} else if (mode.equalsIgnoreCase("co")) {
			return "complement";
		} else if (mode.equalsIgnoreCase("2D")) {
			return mode;
		} else {
			return "unknown";
		}
	}

	//-----------------------------------------------------------------------//
	// Read the given binary file, and return its contents as a byte array   //
	//-----------------------------------------------------------------------//
	public static byte[] read(File file) {
		System.out.println("Reading in binary file named : " + file.getAbsolutePath());
		System.out.println("File size: " + file.length());
		byte[] result = new byte[(int)file.length()];
		try {
			InputStream input = null;
			try {
				int totalBytesRead = 0;
				input = new BufferedInputStream(new FileInputStream(file));
				while(totalBytesRead < result.length){
					int bytesRemaining = result.length - totalBytesRead;
					//input.read() returns -1, 0, or more :
					int bytesRead = input.read(result, totalBytesRead, bytesRemaining); 
					if (bytesRead > 0){
						totalBytesRead = totalBytesRead + bytesRead;
					}
				}
				// the above style is a bit tricky: it places bytes into the 'result' array; 
				// 'result' is an output parameter;
				// the while loop usually has a single iteration only.
				System.out.println("Num bytes read: " + totalBytesRead);
			}
			finally {
				System.out.println("Error: Closing input stream.");
				input.close();
			}
		}
		catch (FileNotFoundException ex) {
			System.out.println("Error: File not found.");
		}
		catch (IOException ex) {
			System.out.println(ex);
		}
		return result;
	}

	public static long date2seconds(String str_date) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
		Date date = (Date) formatter.parse(str_date); 
		long seconds = date.getTime() / 1000;

		System.out.println(str_date + " = " + seconds);
		return seconds;
	}

	public static void setStatsByInfo(String[] lines, int i, long startTime, BasicStats stats) {
		String v;
		int value;

		// seq_length
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.accSeqLength = value;
			stats.minSeqLength = value;
			stats.maxSeqLength = value;
			stats.lengthMap.put(value, 1);
			if (startTime > -1) {
				stats.yieldMap.put(startTime, value);
			}
		}

		// A
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numA = value;
		}

		// T
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numT = value;
		}
		// G
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numG = value;
		}
		// C
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numC = value;
		}

		// N
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numN = value;
		}		
	}

	public static JFreeChart createHistogram(ArrayList<Double> values, int start, int inc,
			String title, String xLabel, String yLabel) {

		final XYSeries series = new XYSeries("");
		for (int i = 0; i < values.size(); i++) {
			series.add(i, values.get(i));
		}
		final XYSeriesCollection dataset = new XYSeriesCollection(series);

		JFreeChart chart = ChartFactory.createHistogram(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);

		return chart;
	}

	public static void saveChart(JFreeChart chart, int width, int height, String fileName) throws IOException {
		File file = new File(fileName);
		ChartUtilities.saveChartAsJPEG(file, chart, width, height);
	}



	public static JFreeChart plotChannelChart(HashMap<Integer, Integer> map,
			String title, String yLabel) {

		int size = 512;
		ArrayList<Double> values = new ArrayList<Double>();
		for(int i = 0; i < size; i++) {
			values.add(0d);			
		}
		for(int key: map.keySet()) {
			values.set(key, (double) map.get(key));
		}
		JFreeChart chart = createHistogram(values, 1, 1, title, "channel", yLabel); 

		NumberAxis domainAxis = (NumberAxis) chart.getXYPlot().getDomainAxis();            
		domainAxis.setRange(1, 512);

		return chart;
	}
	public static JFreeChart plotHistogram(HashMap<Integer, Integer> map, String title, String xLabel, String yLabel) {

		final XYSeries series = new XYSeries("");
		for(int key: map.keySet()) {
			series.add(key, (double) map.get(key));
		}
		final XYSeriesCollection dataset = new XYSeriesCollection(series);
		 
       
		JFreeChart chart = ChartFactory.createHistogram(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);
		NumberAxis domainAxis = (NumberAxis) chart.getXYPlot().getDomainAxis(); 
		domainAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		return chart;
	}
	public static JFreeChart plotHistogramFloat(HashMap<Float, Integer> map, String title, String xLabel, String yLabel) {

		final XYSeries series = new XYSeries("");
		for(float key: map.keySet()) {
			series.add(key, (int) map.get(key));
		}
		final XYSeriesCollection dataset = new XYSeriesCollection(series);

		JFreeChart chart = ChartFactory.createHistogram(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);
		NumberAxis domainAxis = (NumberAxis) chart.getXYPlot().getDomainAxis(); 
		domainAxis.setRange(0, 100);
		return chart;
	}

	public static JFreeChart plotCumulativeChart(HashMap<Long, Integer> map, String title, String xLabel, String yLabel) {	
		Map<Long, Integer> treeMap = new TreeMap<Long, Integer>(map);

		final XYSeries series = new XYSeries("");
		double acc = 0, start = 0;
		for(long key: treeMap.keySet()) {
			acc += (double) treeMap.get(key);
			series.add((start == 0 ? 0 : key - start), acc);
			if (start == 0) {
				start = key;
			}
		}

		final XYSeriesCollection dataset = new XYSeriesCollection(series);

		JFreeChart chart = ChartFactory.createXYLineChart(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);

		return chart;
	}

	public static JFreeChart plotSignalChart(HashMap<Double, Double> map,
			String title, String yLabel, String xLabel) {

		Map<Double, Double> treeMap = new TreeMap<Double, Double>(map);

		final XYSeries series = new XYSeries("");
		double yPrev = 0, xStart = 0;
		double x = 0, y = 0;
		for(double key: treeMap.keySet()) {
			x = key;
			if (yPrev > 0) {
				series.add(x - xStart, yPrev);
				
			} else {
				xStart = x;
			}
			y = treeMap.get(x);
			series.add((x - xStart), y);
			
			yPrev = y;
		}

		final XYSeriesCollection dataset = new XYSeriesCollection(series);

		JFreeChart chart = ChartFactory.createXYLineChart(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);

		NumberAxis domainAxis = (NumberAxis) chart.getXYPlot().getDomainAxis();            
		domainAxis.setRange(0, x - xStart);

		return chart;
	}

	public static JFreeChart plotXYChart(HashMap<Integer, Integer> map, String title, String xLabel, String yLabel) {	
		Map<Integer, Integer> treeMap = new TreeMap<Integer, Integer>(map);

		final XYSeries series = new XYSeries("");
		double acc = 0; // start = 0;
		for(int key: treeMap.keySet()) {
			acc = (int) treeMap.get(key);
			series.add(key , acc);
			/*series.add((start == 0 ? 0 : key - start), acc);
			if (start == 0) {
				start = key;
			}*/
		}

		final XYSeriesCollection dataset = new XYSeriesCollection(series);

		JFreeChart chart = ChartFactory.createXYLineChart(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);

		return chart;
	}
	public static JFreeChart plotXYChartFloat(HashMap<Float, Integer> map, String title, String xLabel, String yLabel) {	
		Map<Float, Integer> treeMap = new TreeMap<Float, Integer>(map);

		final XYSeries series = new XYSeries("");
		double acc = 0;
		for(float key: treeMap.keySet()) {
			acc = (int) treeMap.get(key);
			series.add(key , acc);
			
		}

		final XYSeriesCollection dataset = new XYSeriesCollection(series);

		JFreeChart chart = ChartFactory.createXYLineChart(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);

		return chart;
	}
	public static JFreeChart plotNtContentChart(HashMap<Integer, ParamsforDraw> map, String title, String xLabel, String yLabel) {	

		final XYSeries series = new XYSeries("% A");
		final XYSeries series2 = new XYSeries("% C");
		final XYSeries series3 = new XYSeries("% G");
		final XYSeries series4 = new XYSeries("% T");
		final XYSeries series5 = new XYSeries("% N");
	
		float pnumA, pnumT, pnumC, pnumG, pnumN;
		int i = 0;
		for(int key: map.keySet()) {
			i++;
			ParamsforDraw p = map.get(key);
			int total = p.numA + p.numC + p.numG + p.numT + p.numN;
			pnumA = 100 * p.numA / total;
			series.add(key , pnumA);
			pnumC = 100 * p.numC / total;
			series2.add(key , pnumC);
			pnumG = 100 * p.numG / total;
			series3.add(key , pnumG);
			pnumT = 100 * p.numT / total;
			series4.add(key , pnumT);
			pnumN = 100 * p.numN / total;
			series5.add(key , pnumN);
		
		}

		final XYSeriesCollection dataset = new XYSeriesCollection();
		dataset.addSeries(series);
		dataset.addSeries(series2);
		dataset.addSeries(series3);
		dataset.addSeries(series4);
		dataset.addSeries(series5);

		JFreeChart chart = ChartFactory.createXYLineChart(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, true, true, false);

		return chart;
	}
	public static String parseAndInitStats(String info, StatsWritable stats) {
		String runId = null;
		long startTime = -1;
		int i, index, channel = -1;

		if (info != null) {
			String v;
			String[] lines = info.split("\n");

			// time_stamp
			v = lines[1].split("\t")[1];
			if (!v.isEmpty()) {
				try {
					startTime = Utils.date2seconds(v);
				} catch (ParseException e) {
					e.printStackTrace();
					startTime = -1;
				}
			}

			// channel
			v = lines[3].split("\t")[1];
			if (!v.isEmpty()) {
				channel = Integer.valueOf(v);
			}

			// run id
			v = lines[11].split("\t")[1];
			if (!v.isEmpty()) {
				runId = new String("run-id-" + v);
			}
/*
			// template, complement and 2D
			index = 13;
			for (i = index; i < lines.length; i++) {
				v = lines[i].split("\t")[0];
				if (v.equalsIgnoreCase("-te")) {
					Utils.setStatsByInfo(lines, i+4, startTime, stats.sTemplate);
				} else if (v.equalsIgnoreCase("-co")) {
					Utils.setStatsByInfo(lines, i+4, startTime, stats.sComplement);
				} else if (v.equalsIgnoreCase("-2d")) {
					Utils.setStatsByInfo(lines, i+3, startTime, stats.s2D);
				}
			}

			long num_nt = stats.sTemplate.maxSeqLength + stats.sComplement.maxSeqLength + stats.s2D.maxSeqLength;

			if (num_nt > 0) {
				// update maps for channel
				stats.rChannelMap.put(channel, 1);
				stats.yChannelMap.put(channel, num_nt);
			}
			*/
		}

		return runId;
	}

	
	
	public static String createSummaryFile(StatsWritable stats, String runId) throws Exception {
		
		String res = new String();

		res += "-----------------------------------------------------------------------";
		res +=" Statistics for run " + runId;
		res +="-----------------------------------------------------------------------";
		
		res += "Template:";
		res += stats.sTemplate.toSummary();
		res += "Complement: ";
		res += stats.sComplement.toSummary();
		res += "2D: ";
		res += stats.s2D.toSummary();
				
		return res;
	}
	
	
	
	
	public static void parseStatsFile(String rawFileName, String outDir) throws Exception {
		PrintWriter writer = new PrintWriter(outDir + "/summary.txt", "UTF-8");

		int i, value;
		String line, runId;
		String[] fields;

		JFreeChart chart;
		HashMap<Integer, Integer> hist;
		int width = 1024;
		int height = 480;

		BufferedReader in = new BufferedReader(new FileReader(new File(rawFileName)));

		while ((line = in.readLine()) != null) {
			// run id	
			fields = line.split("\t");
			runId = fields[0].substring(7);
			writer.println("-----------------------------------------------------------------------");
			writer.println(" Statistics for run " + runId);
			writer.println("-----------------------------------------------------------------------");

			// skip
			in.readLine();

			// plot: channel vs num. reads
			hist = new HashMap<Integer, Integer>();

			line = in.readLine();
			value = Integer.parseInt(line);
			if (value > 0) {
				for (i = 0; i < value; i++) {
					line = in.readLine();
					fields = line.split("\t");
					hist.put(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
				}
				chart = Utils.plotChannelChart(hist, "Number of reads per channel", "reads");
				Utils.saveChart(chart, width, height, outDir + "/" + runId + "_channel_reads.jpg");
			}

			// skip
			in.readLine();

			// plot: channel vs yield
			hist = new HashMap<Integer, Integer>();

			line = in.readLine();
			value = Integer.parseInt(line);
			if (value > 0) {
				for (i = 0; i < value; i++) {
					line = in.readLine();
					fields = line.split("\t");
					hist.put(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
				}
				chart = Utils.plotChannelChart(hist, "Yield per channel", "yield (nucleotides)");
				Utils.saveChart(chart, width, height, outDir + "/" + runId + "_channel_yield.jpg");
			}

			for (int j = 0; j < 3; j++) {
				String label = null;
				line = in.readLine();
				fields = line.split("\t");
				if (fields[0].equalsIgnoreCase("-te")) {
					label = new String("template");
					writer.println("\nTemplate:");
				} else if (fields[0].equalsIgnoreCase("-co")) {
					label = new String("complement");
					writer.println("\nComplement:");					
				} else if (fields[0].equalsIgnoreCase("-2d")) {
					label = new String("2d");
					writer.println("\n2D:");
				}

				// num. seqs
				line = in.readLine();
				int numSeqs = Integer.parseInt(line);
				writer.println("\tNum. seqs: " + numSeqs);

				if (numSeqs > 0) {
					// total length
					line = in.readLine();
					int totalLength = Integer.parseInt(line);
					writer.println("\tNum. nucleotides: " + totalLength);
					writer.println();
					writer.println("\tMean read length: " + totalLength / numSeqs);

					// min read length
					line = in.readLine();
					value = Integer.parseInt(line);
					writer.println("\tMin. read length: " + value);

					// max read length
					line = in.readLine();
					value = Integer.parseInt(line);
					writer.println("\tMax. read length: " + value);

					writer.println();
					writer.println("\tNucleotides content:");

					// A
					line = in.readLine();
					value = Integer.parseInt(line);
					writer.println("\t\tA: " + value + " (" + (100.0f * value / totalLength) + " %)");

					// T
					line = in.readLine();
					value = Integer.parseInt(line);
					writer.println("\t\tT: " + value + " (" + (100.0f * value / totalLength) + " %)");

					// G
					line = in.readLine();
					value = Integer.parseInt(line);
					writer.println("\t\tG: " + value + " (" + (100.0f * value / totalLength) + " %)");
					int numGC = value;

					// C
					line = in.readLine();
					value = Integer.parseInt(line);
					writer.println("\t\tC: " + value + " (" + (100.0f * value / totalLength) + " %)");
					numGC += value;

					// N
					line = in.readLine();
					value = Integer.parseInt(line);
					writer.println("\t\tN: " + value + " (" + (100.0f * value / totalLength) + " %)");

					writer.println();
					writer.println("\t\tGC: " + (100.0f * numGC / totalLength) + " %");

					// plot: read length vs frequency
					HashMap<Integer, Integer> histread = new HashMap<Integer, Integer>();

					line = in.readLine();
					value = Integer.parseInt(line);
					if (value > 0) {
						for (i = 0; i < value; i++) {
							line = in.readLine();
							fields = line.split("\t");
							histread.put(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
						}
						chart = Utils.plotHistogram(histread, "Read length histogram (" + label + ")", "read length", "frequency");
						Utils.saveChart(chart, width, height, outDir + "/" + runId + "_" + label + "_read_length.jpg");
					}

					// plot: time vs yield
					HashMap<Long, Integer> histLong = new HashMap<Long, Integer>();

					line = in.readLine();
					value = Integer.parseInt(line);
					if (value > 0) {
						for (i = 0; i < value; i++) {
							line = in.readLine();
							fields = line.split("\t");
							histLong.put(Long.valueOf(fields[0]), Integer.valueOf(fields[1]));
						}
						chart = Utils.plotCumulativeChart(histLong, "Cumulative yield (" + label + ")", "time (seconds)", "yield (cumulative nucleotides)");
						Utils.saveChart(chart, width, height, outDir + "/" + runId + "_" + label + "_yield.jpg");
					}
				}
			}

			break;
		}
		in.close();
		writer.close();
	}
	/****************************
	 * 
	 * @param name param for take
	 * @param info info of the file
	 * @return String with the param
	 */
	public static String getValue(String name, String info) {
		
	
			String[] lines = info.split("\n");
			String[] fields; 
			int i = 0;
			fields= lines[i].split("\t");
			while (!name.equals(fields[0]) ){
				fields= lines[i].split("\t");
				i++;
			}
		
		return fields[1];
	}
	/*******************************************************************************
	 * Parse the info with the fastq
	 * @param info
	 * @param stats
	 *******************************************************************************/
	public static void parseAndInitStats(String info, String fastq, StatsWritable stats){
		
		String timeStamp = Utils.getValue("time_stamp", info);
		
		String secuence, quality;
		String[] lines = fastq.split("\n");
		String[] field;		
		
		String channel = Utils.getValue("channel_number", info);
		
		if (lines.length < 5) {
			System.out.println("Warning: None FastQ sequences found!");
		} else {
			int numSeq = 0;
			int numNucleo = 0;
			for (int i = 0; i< lines.length;i+=5){
				secuence =lines[i+2];
				//System.out.println("La secuencia es: "+ secuence);
				quality = lines[i+4];
				numSeq++;
				numNucleo += secuence.length();
				field =lines[i].split("-");
				if (field[1].equals("te")){
					
					countletters(secuence, stats.sTemplate);
					qualitycount(quality, stats.sTemplate);
					updateTime(timeStamp,stats.sTemplate);
				}else if(field[1].equals("co")){
					
					countletters(secuence, stats.sComplement);
					qualitycount(quality, stats.sComplement);
					updateTime(timeStamp,stats.sComplement);
				}else{
					
					countletters(secuence, stats.s2D);
					qualitycount(quality, stats.s2D);
					updateTime(timeStamp,stats.s2D);
				}
				
			}
			
			stats.rChannelMap.put(Integer.parseInt(channel),numSeq);
			stats.yChannelMap.put(Integer.parseInt(channel), numNucleo);
		}
		
	}
	
	public static void countletters(String secuence, BasicStats basicstat){
		/*int count[] = new int [256];
		for (int i = 0; i< secuence.length();i++)
			count[secuence.charAt(i)]++;
			basicstat.numA = count['a']+count['A'];
			basicstat.numT = count['T']+count['t'];
			basicstat.numG = count['G']+count['g'];
			basicstat.numC = count['C']+count['c'];
			basicstat.numN = count['N']+count['n'];
			*/
		
		//int count[] = new int [256];
		//System.out.println("********************** El tamaño de la secuencia es" + secuence.length());
		
		for (int i = 0; i< secuence.length();i++){
			ParamsforDraw p = new ParamsforDraw();
			if((secuence.charAt(i) == 'a')|| (secuence.charAt(i) == ('A'))){
				basicstat.numA++;
				p.numA++;
			}else if((secuence.charAt(i) == 't') || (secuence.charAt(i) == ('T'))){
				basicstat.numT++;
				p.numT++;
			}else if((secuence.charAt(i) == 'g') || (secuence.charAt(i) == ('G'))){
				basicstat.numG++;
				p.numG++;
			}else if((secuence.charAt(i) == 'c') || (secuence.charAt(i) == ('C'))){
				basicstat.numC++;
				p.numC++;
			}else {
				basicstat.numN++;
				p.numN++;
			}
			basicstat.accumulators.put(i, p);
		}
		basicstat.numSeqs++;
		basicstat.accSeqLength = secuence.length();
		basicstat.minSeqLength = secuence.length();
		basicstat.maxSeqLength = secuence.length();
		
		basicstat.lengthMap.put(secuence.length(), 1);

		float percent = (basicstat.numG+basicstat.numC)*100/secuence.length();
		basicstat.numgc.put(percent, 1);
		
		
	}
	public static void qualitycount(String quality, BasicStats basicstats){
		
		int media =0;
		ParamsforDraw p = new ParamsforDraw();
		for (int i = 0; i < quality.length();i++){
			media += quality.charAt(i);
			p.cumul_qual = quality.charAt(i);
			p.frequency = 1;

			basicstats.updateParams(basicstats,i,p);
		}
		media = media/quality.length(); 
		basicstats.meanqualitys = media;
		// FALTA INCLUIR BASICSTATS.COMOSELLAME.CNUL_QUAL = MEDIA;
		System.out.println("La media de la calidad es: " + media);
		
	}
	public static void updateTime(String timeStamp, BasicStats basicstats){
		try {
			long time = Utils.date2seconds(timeStamp);
			basicstats.yieldMap.put(time, basicstats.accSeqLength);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
