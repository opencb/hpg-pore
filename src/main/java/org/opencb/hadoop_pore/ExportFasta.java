package org.opencb.hadoop_pore;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExportFasta extends Configured implements Tool {
	
	public static class Map extends Mapper<Text, BytesWritable, NullWritable, Text> {
		
		private MultipleOutputs<NullWritable, Text> multipleOutputs = null; 
		
		@Override
		public void setup(Context context) {
			NativePoreSupport.loadLibrary();
			
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			multipleOutputs.close();
		}		

		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			System.out.println("***** map: key = " + key);

			String info = new NativePoreSupport().getFastqs(value.getBytes());

			if (info.length() <= 0) return;
			
			byte[] brLine = new byte[1];
			brLine[0] = '\n';
			
			String[] lines = info.split("\n");
			String name = null;
			Text content = null;
			
			String line;
			int i = 0;
			
			System.out.println("info length = " + info.length() + ", num.lines = " + lines.length);
			
			while(true) {
				// first line: runId & template/complement/2d				
				if (i >= lines.length) break;				
				line = lines[i++];
				System.out.println(i + " of " + lines.length + " : " + line);
				name = new String(line);
				
				// second line: read ID
				if (i >= lines.length) break;				
				line = lines[i++];
				content = new Text("> " + line.substring(1) + "\n");
				
				// third line: nucleotides
				if (i >= lines.length) break;				
				line = lines[i++];
				content.append(line.getBytes(), 0, line.length());

				// fourth line: +
				if (i >= lines.length) break;				
				line = lines[i++];

				// fifth line: qualities
				if (i >= lines.length) break;				
				line = lines[i++];

				multipleOutputs.write(NullWritable.get(), content, name);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "hadoop-pore-fasta");
		job.setJarByClass(ExportFasta.class);

		String srcFileName = args[1];
		String outDirName = args[2];

		// add input files to mapreduce processing
		FileInputFormat.addInputPath(job, new Path(srcFileName));
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set output file
		FileOutputFormat.setOutputPath(job, new Path(outDirName));

		// set map, combine, reduce...
		job.setMapperClass(ExportFasta.Map.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	//-----------------------------------------------------------------------//
	// 	                     F A S T A     C O M M A N D                     //
	//-----------------------------------------------------------------------//
	
	public static void fasta(String[] args) throws Exception {	
		if (args.length != 3) {
			System.out.println("Error: Mismatch parameters for fasta command");
			fastaHelp();
			System.exit(-1);
		}

		String inHdfsFilename = args[1];
		String outLocalDirname = new String(args[2]);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if (!fs.exists(new Path(inHdfsFilename))) {
			System.out.println("Error: Hdfs file " + inHdfsFilename + " does not exist!");
			System.exit(-1);			
		}
		
		String outHdfsDirname = new String(inHdfsFilename + "-" + new Date().getTime());
		System.out.println(inHdfsFilename + ", " + outLocalDirname + ", " + outHdfsDirname);
		args[2] = new String(outHdfsDirname);
				
		// map-reduce
		int error = ToolRunner.run(new ExportFasta(), args);
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
        		
        		outLocalRunIdDirname = new String(outLocalDirname + "/" + runId);
        		File outDir = new File(outLocalRunIdDirname);
        		if (!outDir.exists()) {
        			outDir.mkdir();
        		}
        		System.out.println("Copying " + Utils.toModeString(mode) + " sequences for run " + runId + " to the local file " + outLocalRunIdDirname + "/" + Utils.toModeString(mode) + ".fa");
        		fs.copyToLocalFile(status[i].getPath(), new Path(outLocalRunIdDirname + "/" + Utils.toModeString(mode) + ".fa"));
        		System.out.println("Done.");
        	}
        }
        fs.delete(new Path(outHdfsDirname), true);
	}
	
	//-----------------------------------------------------------------------//

	public static void fastaHelp() {
		System.out.println("fasta command:");
		System.out.println("\thadoop jar hadoop-nano.jar fasta <source> <destination>");
		System.out.println("Options:");
		System.out.println("\tsource     : hadoop hdfs file");
		System.out.println("\tdestination: local destination folder");
	}
	
	//-----------------------------------------------------------------------//
}
