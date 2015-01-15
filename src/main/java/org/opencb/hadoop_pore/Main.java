package org.opencb.hadoop_pore;

public class Main {
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0) {
			System.out.println("Error: Missing command");
			help();
			System.exit(0);
		}
		
		String cmd = args[0];
		
		if (cmd.equalsIgnoreCase("import-fast5")) {
			ImportFast5.run(args);
		} else if (cmd.equalsIgnoreCase("compute-stats")){
			ComputeStats.compute(args);
		} else {
			System.out.println("Error: Unknown command");
			help();
			System.exit(0);
		}
		/*
		int ecode = ToolRunner.run(new MultipleInputFile(), args);


		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path outFile = new Path(args[2] + "/part-r-00000");
		System.out.println("out file = " + outFile.getName());

		if (!fs.exists(outFile)) {
			System.out.println("out file = " + outFile.getName() + " does not exist !!");
		} else {			
			FSDataInputStream in = fs.open(outFile);
			int bytesRead;
			byte[] buffer = new byte[2038];
			while ((bytesRead = in.read(buffer)) > 0) {
				System.out.println("num. reads bytes = " + bytesRead);
				for (int i = 0; i < bytesRead; i++) {
					System.out.print((char) buffer[i]);
				}
			}
			in.close();
		}

		new HistogramGraph();

		System.exit(ecode);
		 */
	}
	
	public static void help() {
		System.out.println("hadoop jar hadoop-nano.jar <commands> <options>");
		System.out.println();
		System.out.println("Commands:");
		System.out.println("\timport-fast5  : Copy the fast5 files into the Hadoop cluster");
		System.out.println("\texport-fastq  : Extract the FastQ sequences from the imported Fast5 files");
		System.out.println("\texport-fasta  : Extract the Fasta sequences from the imported Fast5 files");
		System.out.println("\tcompute-stats : Compute statistics and generate some plots for the imported Fast5 files");
		System.out.println("\tplot-signal   : Plot the sequencer's electronic signal over the time for a given Fast5 file"); 
		System.out.println();
		
										/*				

						hadoop jar hadoop-nano.jar export-fastq <hdfs id> <destination local folder> [run id]
						hadoop jar hadoop-nano.jar export-fasta <hdfs id> <destination local folder> [run id]

						hadoop jar hadoop-nano.jar plot-signal <hdfs id> <fast5 file name> <destination local folder> [min seconds] [max seconds]
										 */
	}
}
