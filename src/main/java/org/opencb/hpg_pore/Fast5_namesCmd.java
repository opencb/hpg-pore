package org.opencb.hpg_pore;

import org.opencb.hpg_pore.commandline.FastaCommandLine;

import com.beust.jcommander.JCommander;

public class Fast5_namesCmd {
	public static String outDir;
	//-----------------------------------------------------------------------//
	// 	                     F A S T A     C O M M A N D                     //
	//-----------------------------------------------------------------------//

	public static void run(String[] args) throws Exception {	

		FastaCommandLine cmdLine = new FastaCommandLine();
		JCommander cmd = new JCommander(cmdLine);
		cmd.setProgramName(Main.BINARY_NAME + " fasta");

		try {
			cmd.parse(args);
		} catch (Exception e) {
			cmd.usage();
			System.exit(-1);
		}

			
	}

}
