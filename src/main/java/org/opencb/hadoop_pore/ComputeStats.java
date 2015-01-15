package org.opencb.hadoop_pore;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class ComputeStats  extends Configured implements Tool {

	public static class MyMap extends Mapper<Text, BytesWritable, Text, StatsWritable> {
		@Override
		public void setup(Context context) {
			System.out.println("-----> loading libs..");
			//System.load(new File("/tmp/libnativefast5.so").getAbsolutePath());
			String hostname;
			File fast5Lib = new File("/tmp/libopencb_pore.so");
			try {
				hostname = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				hostname = new String("no-name");
			}
			if (fast5Lib.exists()) {
				System.out.println("*********** " + fast5Lib.getAbsolutePath() + " exists (" + hostname + ")");
			} else {
				System.out.println("*********** " + fast5Lib.getAbsolutePath() + " does NOT exist (" + hostname + ")");				
			}
			System.load(fast5Lib.getAbsolutePath());
		}

		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			System.out.println("***** map: key = " + key);

			String info = new NativePoreSupport().getInfo(value.getBytes());
			StatsWritable finalStats = new StatsWritable();

			if (info != null) {
				/*
				String[] lines = fastqs.split("\n");
				for (int i = 1; i < lines.length; i+=4) {
					StatsWritable stats = new StatsWritable();
					char[] sequence = lines[i].toCharArray();
					stats.minSeqLength = sequence.length;
					stats.accSeqLength = sequence.length;
					stats.maxSeqLength = sequence.length;
					stats.lengthMap.put(sequence.length, 1);
					for (char nt: sequence) {
						switch (nt) {
						case 'A':
						case 'a':
							stats.numA++;
							break;
						case 'T':
						case 't':
							stats.numT++;
							break;
						case 'G':
						case 'g':
							stats.numG++;
							break;
						case 'C':
						case 'c':
							stats.numC++;
							break;
						case 'N':
						case 'n':
							stats.numN++;
							break;
						}
					}
					finalStats.update(stats);
				}
				 */
			}
			System.out.println("+++++ from map: stats:\n" + finalStats.toString());
			context.write(new Text("hello"), finalStats);
		}
	}

	public static class MyCombine extends Reducer<Text, StatsWritable, Text, StatsWritable> {
		@Override
		public void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
			StatsWritable finalStats = new StatsWritable();

			for (StatsWritable stat: values) {
				//finalStats.update(stat);
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
				//finalStats.update(stat);
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

/*
		try {  
			String str_date = "2014-08-25 05:49:51";
			//String str_date="11-June-07";
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date date = (Date) formatter.parse(str_date); 
			long seconds1 = date.getTime() / 1000;

			str_date = "2014-08-25 05:51:51";
			formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			date = (Date) formatter.parse(str_date); 
			long seconds2 = date.getTime() / 1000;
			
			System.out.println(seconds2 + " - " + seconds1 + " = " + (seconds2 - seconds1));
		} catch (ParseException e)	{
			System.out.println("Exception :" + e);  
		}  

		System.exit(0);
*/
		if (args.length != 3) {
			System.out.println("Error: Mismatch parameters for stats command");
			importHelp();
			System.exit(0);
		}

		int ecode = ToolRunner.run(new ComputeStats(), args);

		HashMap<Integer, Integer> hist = new HashMap<Integer, Integer>();
		hist.put(100, 3);
		hist.put(150, 5);
		hist.put(20, 1);
		hist.put(120, 5);
		hist.put(110, 10);

		HistogramGraph graph = new HistogramGraph(hist);
		graph.save("/tmp/lentgh_hist.png");

	}

	//-----------------------------------------------------------------------//

	public static void importHelp() {
		System.out.println("compute-stats command:");
		System.out.println("\thadoop jar hadoop-nano.jar compute-stats <source> <destination>");
		System.out.println("Options:");
		System.out.println("\tsource     : hdfs file where you imported the fast5 files");
		System.out.println("\tdestination: destination local folder to save stats, plots,...");
	}	
}
