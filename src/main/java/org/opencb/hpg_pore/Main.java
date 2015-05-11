package org.opencb.hpg_pore;

import org.opencb.hpg_pore.hadoop.HadoopImportCmd;

public class Main {
	public static final String BINARY_NAME = "hpg-pore";
	public static final String VERSION = "0.1.0";
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Error: Missing command");
			help();
			System.exit(0);
		}

		String cmd = args[0];

		String[] newArgs = new String[args.length - 1]; 
		for(int i = 1; i < args.length; i++) {
			newArgs[i-1] = new String(args[i]);
		}	

		if (cmd.equalsIgnoreCase("import")) {
			HadoopImportCmd.run(newArgs);
		} else if (cmd.equalsIgnoreCase("stats")){
			StatsCmd.run(newArgs);
		} else if (cmd.equalsIgnoreCase("fastq")){
			FastqCmd.run(newArgs);
		} else	if (cmd.equalsIgnoreCase("fasta")){
			FastaCmd.run(newArgs);
		} else	if (cmd.equalsIgnoreCase("help")){
			help();
		} else	if (cmd.equalsIgnoreCase("version")){
			version();
		} else if (cmd.equalsIgnoreCase("squiggle")) {
			SquiggleCmd.run(newArgs);
		} else if (cmd.equalsIgnoreCase("events")) {
			EventsCmd.run(newArgs);
		} else if (cmd.equalsIgnoreCase("fast5names")) {
			Fast5NamesCmd.run(newArgs);
		} else if (cmd.equalsIgnoreCase("export")) {
			ExportCmd.run(newArgs);
		} else {
			System.out.println("Error: Unknown command");
			help();
			System.exit(0);
		}
	}

	public static void help() {
		System.out.println("Usage: " + BINARY_NAME + " COMMAND");
		System.out.println("	   where COMMAND is one of:");
		System.out.println();
		System.out.println("\tstats        explore Fast5 reads by computing statistics and plotting charts");
		System.out.println("\tsquiggle     plot the measured signal for a given Fast5 read");
		System.out.println("\tevents       extract the events");
		System.out.println("\tfastq        extract the sequences in Fastq format for a set of Fast5 reads");
		System.out.println("\tfasta        extract the sequences in Fasta format for a set of Fast5 reads");
		System.out.println("\tfast5names   extract the names of files in HFDS directory");
		System.out.println();
		System.out.println("Previous commands can run both on a local system and on a Hadoop environment (for the latter, use the option --hadoop.");
		System.out.println("Before executing those commands on a Hadoop environment, you must copy your Fast5 files to the Hadoop file system by running the command:");
		System.out.println();
		System.out.println("\timport    copy the Fast5 files into the Hadoop environment (a HDFS Hadoop file)");
		System.out.println();
		System.out.println("If you want to get back your Fast5 files imported to the Hadoop environment, use the command:e Hadoop file system by running the command:");
		System.out.println();
		System.out.println("\texport   	copy back your Fast5 files in the local filesystem from the Hadoop environment");
		System.out.println();
		System.out.println("Other commands:");
		System.out.println();
		System.out.println("\tversion   print the version");
		System.out.println("\thelp      print this help");
		System.out.println();
		System.out.println("Most commands print help when invoked w/o parameters.");
	}
	
	public static void version() {
		System.out.println("Version: " + BINARY_NAME + " " + VERSION);
	}

}
