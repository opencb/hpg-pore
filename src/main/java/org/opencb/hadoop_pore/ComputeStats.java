package org.opencb.hadoop_pore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

public class ComputeStats  extends Configured implements Tool {

	public static class MyMap extends Mapper<Text, BytesWritable, Text, StatsWritable> {
		@Override
		public void setup(Context context) {
			System.out.println("-----> loading libs..");
			//System.load(new File("/tmp/libnativefast5.so").getAbsolutePath());
			String hostname;
			File poreLib = new File("/tmp/libopencb_pore.so");
			try {
				hostname = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				hostname = new String("no-name");
			}
			if (poreLib.exists()) {
				System.out.println("*********** " + poreLib.getAbsolutePath() + " exists (" + hostname + ")");
			} else {
				System.out.println("*********** " + poreLib.getAbsolutePath() + " does NOT exist (" + hostname + ")");				
			}
			System.load(poreLib.getAbsolutePath());
		}

		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			System.out.println("***** map: key = " + key);

			String info = new NativePoreSupport().getInfo(value.getBytes());

			Text runId = new Text("run-id-unknown");
			StatsWritable stats = new StatsWritable();

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
					runId = new Text("run-id-" + v);
				}

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

			}
			System.out.println("(start time, channel, run id) = (" + startTime + ", " + channel + ", " + runId + ")");
			System.out.println("+++++ from map: stats:\n" + stats.toString());
			context.write(runId, stats);
		}
	}

	public static class MyCombine extends Reducer<Text, StatsWritable, Text, StatsWritable> {
		@Override
		public void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
			StatsWritable finalStats = new StatsWritable();

			for (StatsWritable stat: values) {
				finalStats.update(stat);
			}

			System.out.println("+++++ from combine: stats:\n" + finalStats.toString());
			context.write(key, finalStats);
		}
	}

	public static class MyReduce extends Reducer<Text, StatsWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("***** reduce: key = " + key);

			StatsWritable finalStats = new StatsWritable();

			for (StatsWritable stat: values) {
				finalStats.update(stat);
			}

			Text res = new Text(finalStats.toFormat());
			System.out.println("+++++ from reduce: stats:\n" + finalStats.toString());
			context.write(key, res);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "hadoop-nano-stats");
		job.setJarByClass(ComputeStats.class);

		String srcFileName = args[1];
		String outDirName = args[2];

		// add input files to mapreduce processing
		FileInputFormat.addInputPath(job, new Path(srcFileName));
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set output file
		FileOutputFormat.setOutputPath(job, new Path(outDirName));

		// set map, combine, reduce...
		job.setMapperClass(MyMap.class);
		//job.setCombinerClass(MyCombine.class);
		job.setReducerClass(MyReduce.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StatsWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	//-----------------------------------------------------------------------//
	// 	                     S T A T S     C O M M A N D                     //
	//-----------------------------------------------------------------------//

	public static void compute(String[] args) throws Exception {	

		{
			HashMap<Double, Double> map = new HashMap<Double, Double>();
/*			
			map.put(42126.65, 106.01);
			map.put(42126.91, 104.28);
			map.put(42126.94, 89.49);
			map.put(42126.96, 95.37);
			map.put(42126.97, 93.61);
			map.put(42126.98, 97.91);
*/

			map.put(42126.649600000004, 106.01406509519562);
			map.put(42126.909, 104.280791015625);
			map.put(42126.935000000005, 89.49470374561915);
			map.put(42126.9564, 95.37134024378766);
			map.put(42126.973000000005, 93.61251678466796);
			map.put(42126.981, 97.90542332848837);
			map.put(42126.9982, 101.80486384901889);
			map.put(42127.154, 103.66451843261717);
			map.put(42127.166000000005, 102.26012236359831);
			map.put(42127.3314, 102.95171609120825);
			map.put(42127.3606, 98.44822291324013);
			map.put(42127.552800000005, 91.07494995117186);
			map.put(42127.5668, 94.50640422276086);
			map.put(42127.5892, 99.72964179256368);
			map.put(42127.784, 98.80370178222657);
			map.put(42127.7944, 93.27634470086348);
			map.put(42127.802, 97.78277201592167);
			map.put(42127.833600000005, 93.55597003439198);
			map.put(42127.8428, 95.94762369791667);

			String outLocalDir = "/tmp/pore";
			int width = 1024;
			int height = 480;

			JFreeChart chart = Utils.plotSignalChart(map, "Signal for XXX, [2-30] sec.", "measured signal", "time");
			Utils.saveChart(chart, width, height, outLocalDir + "/signal.jpg");
			System.exit(0);
		}

		if (args.length != 3) {
			System.out.println("Error: Mismatch parameters for stats command");
			statsHelp();
			System.exit(0);
		}
		int ecode = ToolRunner.run(new ComputeStats(), args);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path outFile = new Path(args[2] + "/part-r-00000");
		System.out.println("out file = " + outFile.getName());

		if (!fs.exists(outFile)) {
			System.out.println("out file = " + outFile.getName() + " does not exist !!");
		} else {
			String outLocalDir = "/tmp/pore";
			String outRawFilename = outLocalDir + "/raw.txt";
			fs.copyToLocalFile(outFile, new Path(outRawFilename));

			PrintWriter writer = new PrintWriter(outLocalDir + "/summary.txt", "UTF-8");

			int i, value;
			String line, runId;
			String[] fields;

			JFreeChart chart;
			HashMap<Integer, Integer> hist;
			int width = 1024;
			int height = 480;

			BufferedReader in = new BufferedReader(new FileReader(new File(outRawFilename)));

			while (true) {
				// run id	
				line = in.readLine();
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
					Utils.saveChart(chart, width, height, outLocalDir + "/" + runId + "_channel_reads.jpg");
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
					Utils.saveChart(chart, width, height, outLocalDir + "/" + runId + "_channel_yield.jpg");
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
					hist = new HashMap<Integer, Integer>();

					line = in.readLine();
					value = Integer.parseInt(line);
					if (value > 0) {
						for (i = 0; i < value; i++) {
							line = in.readLine();
							fields = line.split("\t");
							hist.put(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
						}
						chart = Utils.plotHistogram(hist, "Read length histogram (" + label + ")", "read length", "frequency");
						Utils.saveChart(chart, width, height, outLocalDir + "/" + runId + "_" + label + "_read_length.jpg");
					}

					// plot: time vs yield
					hist = new HashMap<Integer, Integer>();

					line = in.readLine();
					value = Integer.parseInt(line);
					if (value > 0) {
						for (i = 0; i < value; i++) {
							line = in.readLine();
							fields = line.split("\t");
							hist.put(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]));
						}
						chart = Utils.plotCumulativeChart(hist, "Cumulative yield (" + label + ")", "time (seconds)", "yield (cumulative nucleotides)");
						Utils.saveChart(chart, width, height, outLocalDir + "/" + runId + "_" + label + "_yield.jpg");
					}
				}

				break;
			}
			in.close();
			writer.close();
		}



		//		HistogramGraph graph = new HistogramGraph(hist);
		//		graph.save("/tmp/lentgh_hist.png");

	}

	//-----------------------------------------------------------------------//

	public static void statsHelp() {
		System.out.println("compute-stats command:");
		System.out.println("\thadoop jar hadoop-nano.jar compute-stats <source> <destination>");
		System.out.println("Options:");
		System.out.println("\tsource     : hdfs file where you imported the fast5 files");
		System.out.println("\tdestination: destination local folder to save stats, plots,...");
	}	
}
