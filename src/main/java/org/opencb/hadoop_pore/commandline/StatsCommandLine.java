package org.opencb.hadoop_pore.commandline;

import com.beust.jcommander.Parameter;

public class StatsCommandLine {
	 
	  @Parameter(names = "--in", description = "Input folder (or file) where to find Fast5 files", required = true)
	  private String in;
	 
	  @Parameter(names = "--out", description = "Output folder where to save statistics results and charts", required = true)
	  private String out;

	  @Parameter(names = "--hadoop", description = "Run the command on a Hadoop environment")
	  private boolean isHadoop = false;

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

	public boolean isHadoop() {
		return isHadoop;
	}

	public void setHadoop(boolean isHadoop) {
		this.isHadoop = isHadoop;
	}
}
