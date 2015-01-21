package org.opencb.hpg_pore.hadoop;

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
import org.apache.hadoop.util.Tool;
import org.opencb.hpg_pore.NativePoreSupport;
import org.opencb.hpg_pore.Utils;

public class HadoopStatsCmd extends Configured implements Tool {

	public static class Map extends Mapper<Text, BytesWritable, Text, StatsWritable> {
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

			Text runId = null;
			StatsWritable stats = new StatsWritable();

			String name = Utils.parseAndInitStats(info, stats);
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

			System.out.println("+++++ from combine: stats:\n" + finalStats.toString());
			context.write(key, finalStats);
		}
	}

	public static class Reduce extends Reducer<Text, StatsWritable, Text, Text> {
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
		Job job = new Job(conf, "hadoop-pore-stats");
		job.setJarByClass(HadoopStatsCmd.class);

		String srcFileName = args[0];
		String outDirName = args[1];

		// add input files to mapreduce processing
		FileInputFormat.addInputPath(job, new Path(srcFileName));
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

	/*
		{
			HashMap<Double, Double> map = new HashMap<Double, Double>();

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
	 */
}
