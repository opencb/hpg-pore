package org.opencb.hpg_pore.hadoop;


import java.io.IOException;
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
import org.opencb.hpg_pore.NativePoreSupport;
import org.opencb.hpg_pore.Utils;

public class HadoopStatsCmd extends Configured implements Tool {
		
	public static class Map extends Mapper<Text, BytesWritable, Text, StatsWritable> {
		@Override
		public void setup(Context context) {
			NativePoreSupport.loadLibrary();
		}

		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			//System.out.println("***** map: key = " + key);

			String info = new NativePoreSupport().getInfo(value.getBytes());
			if (info == null || info.length() <= 0) {
				System.out.println("Error reading file . Maybe, the file is corrupt.");
				return;
			}

			String fastqs = new NativePoreSupport().getFastqs(value.getBytes());
			Text runId = null;
			StatsWritable stats = new StatsWritable();

			String name = Utils.getValue("run_id", info);
			Utils.parseAndInitStats(info, fastqs, stats);
			
			if (name != null) {
				runId = new Text(name);
			} else {
				runId = new Text("run-id-unknown");
			}
			
				
			context.write(runId, stats);
		}
	}

	public static class Combine extends Reducer<Text, StatsWritable, Text, StatsWritable> {
		@Override
		public void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
			StatsWritable finalStats = new StatsWritable();

			for (StatsWritable stat: values) {
				finalStats.update(stat);
			}

			//System.out.println("+++++ from combine: stats:\n" + finalStats.toString());
			context.write(key, finalStats);
		}
	}

	public static class Reduce extends Reducer<Text, StatsWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<StatsWritable> values, Context context) throws IOException, InterruptedException {
			//System.out.println("***** reduce: key = " + key);
			StatsWritable finalStats = new StatsWritable();
			for (StatsWritable stat: values) {
				finalStats.update(stat);
			}

			Text res = new Text(finalStats.toFormat());
			//System.out.println("+++++ from reduce: stats:\n" + finalStats.toString());
			
			context.write(key, res);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "hpg-pore-stats");
		job.setJarByClass(HadoopStatsCmd.class);

		job.addCacheFile(new Path(NativePoreSupport.LIB_FULLNAME).toUri());

		String srcFileName = args[0];
		String outDirName = args[1];
		
		// add input files to mapreduce processing
		FileInputFormat.addInputPath(job, new Path(srcFileName + "/data"));
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set output file
		FileOutputFormat.setOutputPath(job, new Path(outDirName));
		
		// set map, combine, reduce...
		job.setMapperClass(HadoopStatsCmd.Map.class);
		job.setCombinerClass(HadoopStatsCmd.Combine.class);
		job.setReducerClass(HadoopStatsCmd.Reduce.class);
		job.setNumReduceTasks(1);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StatsWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
