package org.opencb.hpg_pore.commandline;

import com.beust.jcommander.Parameter;

public class Fast5NamesCommandLine {
	@Parameter(names = "--in", description = "Input folder (or file) where to find Fast5 files", required = true)
	private String in;

	@Parameter(names = "--out", description = "Output folder where to save the file with all names", required = true)
	private String out;

	@Parameter(names = "--hadoop", description = "Run the command on a Hadoop environment")
	private boolean isHadoop = false;


	public String getin() {
		return in;
	}

	public void setin(String in) {
		this.in = in;
	}

	public String getOut() {
		return out;
	}

	public void setOut(String out) {
		this.out = out;
	}

	public void setHadoop(boolean isHadoop) {
		this.isHadoop = isHadoop;
	}
	public boolean isHadoop() {
		return isHadoop;
	}
}
