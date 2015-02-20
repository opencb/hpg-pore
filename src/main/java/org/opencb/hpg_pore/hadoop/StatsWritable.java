package org.opencb.hpg_pore.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.jfree.chart.JFreeChart;
import org.opencb.hpg_pore.Utils;

public class StatsWritable implements Writable {

	public HashMap<Integer, Integer> rChannelMap;
	public HashMap<Integer, Integer> yChannelMap;

	public BasicStats sTemplate;
	public BasicStats sComplement;
	public BasicStats s2D;

	public StatsWritable() {
		rChannelMap = new HashMap<Integer, Integer>();
		yChannelMap = new HashMap<Integer, Integer>();

		sTemplate = new BasicStats();
		sComplement = new BasicStats();
		s2D = new BasicStats();
	}

	public void update(StatsWritable stats) {

		int v1 = 0;
		for(Object key:stats.rChannelMap.keySet()) {
			v1 = stats.rChannelMap.get((Integer) key);
			if (rChannelMap.containsKey((Integer) key)) {
				v1 += rChannelMap.get((Integer) key);
			}
			rChannelMap.put((Integer) key, v1);
		}

		int v2 = 0;
		for(Object key:stats.yChannelMap.keySet()) {
			v2 = stats.yChannelMap.get((Integer) key);
			if (yChannelMap.containsKey((Integer) key)) {
				v2 += yChannelMap.get((Integer) key);
			}
			yChannelMap.put((Integer) key, v2);
		}

		sTemplate.update(stats.sTemplate);
		sComplement.update(stats.sComplement);
		s2D.update(stats.s2D);
	}

	public void readFields(DataInput in) throws IOException {
		int size;

		rChannelMap = new HashMap<Integer, Integer>();
		size = in.readInt();
		for (int i = 0; i < size; i++) {
			rChannelMap.put(in.readInt(), in.readInt());
		}

		yChannelMap = new HashMap<Integer, Integer>();
		size = in.readInt();
		for (int i = 0; i < size; i++) {
			yChannelMap.put(in.readInt(), in.readInt());
		}

		sTemplate.readFields(in);
		sComplement.readFields(in);
		s2D.readFields(in);		
	}

	public void write(DataOutput out) throws IOException {

		out.writeInt(rChannelMap.size());
		for(Object key:rChannelMap.keySet()) {
			out.writeInt((Integer) key);
			out.writeInt((Integer) rChannelMap.get(key));
		}

		out.writeInt(yChannelMap.size());
		for(Object key:yChannelMap.keySet()) {
			out.writeInt((Integer) key);
			out.writeInt((Integer) yChannelMap.get(key));
		}

		sTemplate.write(out);
		sComplement.write(out);
		s2D.write(out);		
	}
	public void draw(String runId, String out){
		int width = 1024;
		int height = 480;
		String[] basics={"Template","Complement","2D"};
		BasicStats[] b = {this.sTemplate, this.sComplement, this.s2D};
		
		File outDir = new File(out + "/" + runId);
		if (!outDir.exists()) {
			if (!outDir.mkdir()) {
				System.out.println("Error creating output forlder: " + outDir.getAbsolutePath());
				System.exit(-1);
			}
		}
		
		/*************************************
		* DRAW READ - CHANNEL
		***********************************/
		JFreeChart chartRC = Utils.plotChannelChart(this.rChannelMap, "Number of reads per channel", "reads");
		
		try {
			Utils.saveChart(chartRC, width, height, out + "/" + runId + "/" + "_read_channel.jpg");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		/*************************************
		* DRAW YIELD - CHANNEL
		***********************************/
		JFreeChart chartYC = Utils.plotChannelChart(this.yChannelMap, "Number of nucleotides per channel", "nucleotides");
		try {
			Utils.saveChart(chartYC, width, height, out + "/" + runId + "/" + "_yield_channel.jpg");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*************************************
		* DRAW READ_LENGTH - FREQUENCY
		***********************************/
		for (int i = 0; i< 3; i++){
			//JFreeChart chart = Utils.plotXYChart(b[i].yieldMap, "Cumulative Yield for" + basics[i], "measured signal", "time");
			JFreeChart chart = Utils.plotHistogram(b[i].lengthMap, "Read length histogram (" + basics[i] + ")", "read length", "frequency");
			try {
				Utils.saveChart(chart, width, height, out + "/" + runId + "/" + "_" + basics[i]+ "_read_length.jpg");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/**********************************
		* DRAW YIELD
		***********************************/
		
		for (int i = 0; i< 3; i++){
			JFreeChart chart = Utils.plotCumulativeChart(b[i].yieldMap, "Cumulative Yield (" + basics[i] + ")",  "time(seconds)","yield (cumulative nucleotides)");
			try {
				Utils.saveChart(chart, width, height, out + "/"+ runId + "/" + basics[i] + "_yield.jpg");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		/***********************************
		* DRAW POS CUMUL_QUAL - FREQUENCY 
		************************************/
		for (int i = 0; i< 3; i++){
			HashMap<Integer, Integer> hist= new HashMap<Integer, Integer>(); //cumul_qual - frequency
			
			for(int key: b[i].accumulators.keySet()) {
				ParamsforDraw d = b[i].accumulators.get(key);
				int c = d.cumul_qual/d.frequency;
				hist.put(key, c);
			}
			JFreeChart chart = Utils.plotXYChart(hist, "Per base sequence quality (" + basics[i] + ")",  "Position in read(bp) ", "Quality Scores");
			try {
				Utils.saveChart(chart, width, height, out + "/"+ runId + "/" + basics[i] + "_qualityperbase.jpg");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/*************************************
		 * DRAW POS - NUMA NUMT NUM C NUMN
		 *************************************/
		for (int i = 0; i< 3; i++){
			JFreeChart chart = Utils.plotNtContentChart(b[i].accumulators,"Per base sequence content("+ basics[i] + ")",  "Position in read(bp) ", "Sequence content");
			try {
				Utils.saveChart(chart, width, height, out + "/"+ runId + "/" + basics[i] + "_sequencecontent.jpg");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/************************************
		 * DRAW %GC FREQUENCY
		 ************************************/
		for (int i= 0; i< 3; i++){
			//JFreeChart chart = Utils.plotXYChartFloat(b[i].numgc, "Frequency - %GC("+ basics[i] + ")", "%GC", "Frequency");
			JFreeChart chart = Utils.plotHistogramFloat(b[i].numgc, "Frequency - %GC("+ basics[i] + ")", "%GC", "Frequency");
			try {
				Utils.saveChart(chart, width, height, out + "/"+ runId + "/" + basics[i] + "_%GC.jpg");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public String toFormat() {
		String res = new String();

		res += "\n";
		res += "reads-per-channel\n";
		res += rChannelMap.size() + "\n";
		for(Object key:rChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Integer) rChannelMap.get(key)) + "\n";
		}

		res += "yield-per-channel\n";
		res += yChannelMap.size() + "\n";
		for(Object key:yChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Integer) yChannelMap.get(key)) + "\n";
		}

		res += "-te\n";
		res += sTemplate.toFormat();
		res += "-co\n";
		res += sComplement.toFormat();
		res += "-2d\n";
		res += s2D.toFormat();

		return res;
	}

	public String toString() {
		String res = new String();

		res += "reads-per-channel\n";
		res += rChannelMap.size() + "\n";
		for(Object key:rChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Integer) rChannelMap.get(key)) + "\n";
		}

		res += "yield-per-channel\n";
		res += yChannelMap.size() + "\n";
		for(Object key:yChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Integer) yChannelMap.get(key)) + "\n";
		}

		res += "-te\n";
		res += sTemplate.toString();
		res += "-co\n";
		res += sComplement.toString();
		res += "-2d\n";
		res += s2D.toString();

		return res;
	}

	public class BasicStats { 

		public int numSeqs;
		public int accSeqLength;
		public int minSeqLength;
		public int maxSeqLength;
		public int numA;
		public int numT;
		public int numG;
		public int numC;
		public int numN;
		public int meanqualitys;
		
		public HashMap<Integer, Integer> lengthMap;
		public HashMap<Long, Integer> yieldMap;
		public HashMap<Integer,ParamsforDraw> accumulators;
		public HashMap<Float,Integer> numgc;
		public BasicStats() {
			numSeqs = 0;
			accSeqLength = 0;
			minSeqLength = Integer.MAX_VALUE;
			maxSeqLength = 0;
			numA = 0;
			numT = 0;
			numG = 0;
			numC = 0;
			numN = 0;
			meanqualitys = 0;
			lengthMap = new HashMap<Integer, Integer>();
			yieldMap = new HashMap<Long, Integer>();
			accumulators = new HashMap<Integer,ParamsforDraw>();
			numgc = new HashMap<Float, Integer>();
			
		}

		public void write(DataOutput out) throws IOException {
			//int length = 9;
			//out.writeInt(length);
			out.writeInt(numSeqs);
			out.writeInt(numA);
			out.writeInt(numT);
			out.writeInt(numG);
			out.writeInt(numC);
			out.writeInt(numN);
			out.writeInt(minSeqLength);
			out.writeInt(accSeqLength);
			out.writeInt(maxSeqLength);

			out.writeInt(lengthMap.size());
			for(Object key:lengthMap.keySet()) {
				out.writeInt((Integer) key);
				out.writeInt((Integer) lengthMap.get(key));
			}

			out.writeInt(yieldMap.size());
			for(Object key:yieldMap.keySet()) {
				out.writeLong((Long) key);
				out.writeInt((Integer) yieldMap.get(key));
			}
			
			out.writeInt(accumulators.size());
			for(Object key:accumulators.keySet()) {
				out.writeInt((Integer) key);
				ParamsforDraw p = (ParamsforDraw) accumulators.get(key);
				p.write(out);
			}
			
			out.writeInt(numgc.size());
			for(Object key:numgc.keySet()) {
				out.writeFloat((Float) key);
				out.writeInt((Integer) numgc.get(key));
				
			}
		}

		public void readFields(DataInput in) throws IOException {
			//int length = in.readInt();

			numSeqs = in.readInt();
			numA = in.readInt();
			numT = in.readInt();
			numG = in.readInt();
			numC = in.readInt();
			numN = in.readInt();
			minSeqLength = in.readInt();
			accSeqLength = in.readInt();
			maxSeqLength = in.readInt();

			int size;

			lengthMap = new HashMap<Integer, Integer>();
			size = in.readInt();
			for (int i = 0; i < size; i++) {
				lengthMap.put(in.readInt(), in.readInt());
			}

			yieldMap = new HashMap<Long, Integer>();
			size = in.readInt();
			for (int i = 0; i < size; i++) {
				yieldMap.put(in.readLong(), in.readInt());
			}
			
			accumulators = new HashMap <Integer, ParamsforDraw>();
			size = in.readInt();
			for (int i = 0; i < size; i++){
				int key = in.readInt();
				ParamsforDraw p = new ParamsforDraw();
				p.readFields(in);
				accumulators.put(key, p);
				
			}
			size = in.readInt();
			for ( int i = 0; i < size; i++){
				numgc.put(in.readFloat(), in.readInt());
			}
		}

		public void update(BasicStats stats) {
			if (stats.accSeqLength > 0) {
				numSeqs++;
				numA += stats.numA;
				numT += stats.numT;
				numG += stats.numG;
				numC += stats.numC;
				numN += stats.numN;
				if (stats.minSeqLength < minSeqLength) minSeqLength = stats.minSeqLength;
				if (stats.maxSeqLength > maxSeqLength) maxSeqLength = stats.maxSeqLength;
				accSeqLength += stats.accSeqLength;
				
				meanqualitys +=stats.meanqualitys;
				int v1 = 0;
				for(Object key:stats.lengthMap.keySet()) {
					v1 = stats.lengthMap.get((Integer) key);
					if (lengthMap.containsKey((Integer) key)) {
						v1 += lengthMap.get((Integer) key);
					}
					lengthMap.put((Integer) key, v1);
				}

				Integer v2 = 0;
				for(Object key:stats.yieldMap.keySet()) {
					v2 = stats.yieldMap.get((Long) key);
					if (yieldMap.containsKey((Long) key)) {
						v2 += yieldMap.get((Long) key);
					}
					yieldMap.put((Long) key, v2);
				}
				 
				for(Object key:stats.accumulators.keySet()) {
					ParamsforDraw v3 = stats.accumulators.get( key);
					if (accumulators.containsKey( key)) {
						v3.updateParams(this.accumulators.get( key));
					}
					accumulators.put((Integer) key, v3);
				}
				Integer v4 = 0;
				for(Object key:stats.numgc.keySet()) {
					v4 = stats.numgc.get((Float) key);
					if (numgc.containsKey((Float) key)) {
						v4 += numgc.get((Float) key);
					}
					numgc.put((Float) key, v4);
				}
				//System.out.println(this.toFormat());
			}
		}
		public void updateParams(BasicStats b,Integer i, ParamsforDraw p) {
			ParamsforDraw v1 = b.accumulators.get(i);
			v1.updateParams(p);
				
		}
		public String toFormat() {
			String res = new String();
			int length = (numA + numT + numG + numC + numN);

			res += numSeqs + "\n";
			if (numSeqs > 0) {
				res += length + "\n";
				res += minSeqLength + "\n";
				res += maxSeqLength + "\n";
				res += numA + "\n";
				res += numT + "\n";
				res += numG + "\n";
				res += numC + "\n";
				res += numN + "\n";
				res += meanqualitys/numSeqs + "\n";
				
				res += lengthMap.size() + "\n";
				
				for(Object key:lengthMap.keySet()) {
					res += ((Integer) key) + "\t" + ((Integer) lengthMap.get(key)) + "\n";
				}

				res += yieldMap.size() + "\n";
				for(Object key:yieldMap.keySet()) {
					res += ((Long) key) + "\t" + ((Integer) yieldMap.get(key)) + "\n";
				}
				res += accumulators.size() + "\n";
				for (int i= 0; i < accumulators.size(); i++){
					ParamsforDraw p = (ParamsforDraw) accumulators.get(i);
					res += i + "\t" + p.cumul_qual+"\t" +p.frequency + "\t" + p.numA+"\t"+p.numT+ "\t"+ p.numC+"\t"+p.numG+"\t"+p.numN+"\n";
				}
				res += numgc.size() + "\n";
				for(Object key:numgc.keySet()) {
					res += ((float) key) + "\t" + ((Integer) numgc.get(key)) + "\n";
				}
			}

			return res;
		}	


		@Override
		public String toString() {
			String res = new String();
			int length = (numA + numT + numG + numC + numN);

			res += "Num. seqs: " + numSeqs + "\n";
			if (numSeqs > 0) {
				res += "Num. total nucleotides: " + length + " (" + accSeqLength + ")\n";
				res += "Seq. length (min, avg, max) = (" + minSeqLength + ", " + String.format("%.2f", 1.0f * accSeqLength / numSeqs) + ", " + maxSeqLength + ")\n";

				res += "A: " + numA + " " + String.format("%.2f", 100.0f * numA / length) + "%, ";
				res += "T: " + numT + " " + String.format("%.2f", 100.0f * numT / length) + "%, ";
				res += "G: " + numG + " " + String.format("%.2f", 100.0f * numG / length) + "%, ";
				res += "C: " + numC + " " + String.format("%.2f", 100.0f * numC / length) + "%, ";
				res += "N: " + numN + " " + String.format("%.2f", 100.0f * numN / length) + "%\n";

				res += "GC: " + String.format("%.2f", 100.0f * (numG + numC) / length) + "% \n";

				if (numSeqs > 1) {
					res += "Read length histogram:\n";
					res += "\tLength\tFrequency\n";
					for(Object key:lengthMap.keySet()) {
						res += "\t" + ((Integer) key) + "\t" + ((Integer) lengthMap.get(key)) + "\n";
					}

					res += "Cummulative yield:\n";
					res += "\tTime (in seconds)\tNum. nt\n";
					for(Object key:yieldMap.keySet()) {
						res += "\t" + ((Long) key) + "\t" + ((Integer) yieldMap.get(key)) + "\n";
					}
					
					res += "Pos: mean Frecuency - numA - numT - numC - numG - numN : \n";
					
					for(int i = 0; i < accumulators.size(); i++){
						ParamsforDraw p = new ParamsforDraw();
						p = accumulators.get(i);
						res += "\t" + i + "\t" + p.numA + "\t" + p.numT + "\t" + p.numC + "\t" + p.numG + "\t" + p.numN;
					}
					res += "%GC histogram:\n";
					for(Object key:numgc.keySet()) {
						res += "\t" + ((Float) key) + "\t" + ((Integer) numgc.get(key)) + "\n";
					}
					
				}
			}

			return res;
		}

	
		public String toSummary(){
			String res = new String();
			int length = (numA + numT + numG + numC + numN);
	
			res += "Num. seqs: " + numSeqs + "\n";
			if (numSeqs > 0) {
				res += "Num. total nucleotides: " + length + " (" + accSeqLength + ")\n";
				res += "Seq. length (min, avg, max) = (" + minSeqLength + ", " + String.format("%.2f", 1.0f * accSeqLength / numSeqs) + ", " + maxSeqLength + ")\n";
				
				res += "Nucleotides content";
				res += "\tA: " + numA + " " + String.format("%.2f", 100.0f * numA / length) + "%, ";
				res += "\tT: " + numT + " " + String.format("%.2f", 100.0f * numT / length) + "%, ";
				res += "\tG: " + numG + " " + String.format("%.2f", 100.0f * numG / length) + "%, ";
				res += "\tC: " + numC + " " + String.format("%.2f", 100.0f * numC / length) + "%, ";
				res += "\tN: " + numN + " " + String.format("%.2f", 100.0f * numN / length) + "%\n";
	
				res += "GC: " + String.format("%.2f", 100.0f * (numG + numC) / length) + "% \n";
	
			}
			return res;
		}
	}
	
	public String parseAndInit(String info) {
		String runId = null;
		long startTime = -1;
		int i, index, channel = -1;

		if (info != null) {
			String v;
			String[] fields;
			String[] lines = info.split("\n");

			// time_stamp
			v = lines[1].split("\t")[1];
			if (!v.isEmpty()) {
				try {
					startTime = Utils.date2seconds(v);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
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
					Utils.setStatsByInfo(lines, i+4, startTime, sTemplate);
				} else if (v.equalsIgnoreCase("-co")) {
					Utils.setStatsByInfo(lines, i+4, startTime, sComplement);
				} else if (v.equalsIgnoreCase("-2d")) {
					Utils.setStatsByInfo(lines, i+3, startTime, s2D);
				}
			}

			long num_nt = sTemplate.maxSeqLength + sComplement.maxSeqLength + s2D.maxSeqLength;

			if (num_nt > 0) {
				// update maps for channel
				rChannelMap.put(channel, 1);
				yChannelMap.put(channel, num_nt);
			}
		*/
		}
		
		return runId;
	}

	
}
