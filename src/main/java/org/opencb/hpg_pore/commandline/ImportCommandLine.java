package org.opencb.hpg_pore.commandline;

import com.beust.jcommander.Parameter;

public class ImportCommandLine {
	 
	  @Parameter(names = "--in", description = "Input folder (or file) where to find Fast5 files", required = true)
	  private String in;
	 
	  @Parameter(names = "--out", description = "Output HDFS Hadoop file where to save Fastq sequences", required = true)
	  private String out;
	  
	  @Parameter(names = "--compress", description = "Compress the output HDFS Hadoop file")
	  private boolean isCompression = false;

	public String getIn() {
		return in;
	}

	public void setIn(String in) {
		this.in = in;
	}

	public String getOut() {
		return out;
	}

	public void setOut(String out) {
		this.out = out;
	}

	public boolean isCompression() {
		return isCompression;
	}

	public void setIsCompression(boolean isCompression) {
		this.isCompression = isCompression;
	}
}
