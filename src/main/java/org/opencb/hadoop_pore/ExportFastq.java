package org.opencb.hadoop_pore;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExportFastq extends Configured implements Tool {
	
	public static class Map extends Mapper<Text, BytesWritable, Text, Text> {
		@Override
		public void setup(Context context) {
			System.out.println("-----> loading libs..");
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

			String info = new NativePoreSupport().getFastqs(value.getBytes());

			if (info.length() <= 0) return;
			
			String[] lines = info.split("\n");
			Text newKey = null;
			Text content = null;
			
			String line;
			int i = 0;
			
			System.out.println("info length = " + info.length() + ", num.lines = " + lines.length);
			
			while(true) {
				// first line: runId & template/complement/2d				
				if (i >= lines.length) break;				
				line = lines[i++];
				System.out.println(i + " of " + lines.length + " : " + line);
				newKey = new Text(line);
				
				// second line: read ID
				if (i >= lines.length) break;				
				line = lines[i++];
				content = new Text(line + "\n");
				
				// third line: nucleotides
				if (i >= lines.length) break;				
				line = lines[i++];
				//content.append(line.getBytes(), 0, line.length());

				// fourth line: +
				if (i >= lines.length) break;				
				line = lines[i++];
				//content.append(line.getBytes(), 0, line.length());

				// fifth line: qualities
				if (i >= lines.length) break;				
				line = lines[i++];
				//content.append(line.getBytes(), 0, line.length());

				context.write(newKey, content);
			}
		}
	}
		
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs mos;
		 
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs(context);
		}
	 	    
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for (Text value: values) {
				mos.write(key, value, key.toString());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}		
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "hadoop-pore-fastq");
		job.setJarByClass(ExportFastq.class);

		String srcFileName = args[1];
		String outDirName = args[2];

		// add input files to mapreduce processing
		FileInputFormat.addInputPath(job, new Path(srcFileName));
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set output file
		FileOutputFormat.setOutputPath(job, new Path(outDirName));

		// set map, combine, reduce...
		job.setMapperClass(ExportFastq.Map.class);
		job.setReducerClass(ExportFastq.Reduce.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	//-----------------------------------------------------------------------//
	// 	                     F A S T Q     C O M M A N D                     //
	//-----------------------------------------------------------------------//
	
	public static void fastq(String[] args) throws Exception {	
		if (args.length != 3) {
			System.out.println("Error: Mismatch parameters for fastq command");
			fastqHelp();
			System.exit(0);
		}

		// map-reduce
		int ecode = ToolRunner.run(new ExportFastq(), args);
		
		// post-processing

	}
	
	//-----------------------------------------------------------------------//

	public static void fastqHelp() {
		System.out.println("fastq command:");
		System.out.println("\thadoop jar hadoop-nano.jar fastq <source> <destination>");
		System.out.println("Options:");
		System.out.println("\tsource     : hadoop hdfs file");
		System.out.println("\tdestination: local destination folder");
	}
	
	//-----------------------------------------------------------------------//
}
