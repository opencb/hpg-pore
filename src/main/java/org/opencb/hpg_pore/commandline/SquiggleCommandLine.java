package org.opencb.hpg_pore.commandline;

import com.beust.jcommander.Parameter;

public class SquiggleCommandLine {

	@Parameter(names = "--in", description = "Input folder (or file) where to find Fast5 files", required = true)
	private String in;

	@Parameter(names = "--out", description = "Output folder where to save statistics results and charts", required = true)
	private String out;

	@Parameter(names = "--hadoop", description = "Run the command on a Hadoop environment")
	private boolean isHadoop = false;

	@Parameter(names = "--min", description = "Min start time for draw the graphic")
	private int min;

	@Parameter(names = "--max", description = "Max time for draw the graphic")
	private int max;

	@Parameter(names = "--fast5_name", description = "The old name of the fast5  the user want to use ")
	private String fast5_name;

	public String getin() {
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
	public void setmin(int min){
		this.min = min;
	}
	public int getmin (){
		return this.min;
	}
	public int getmax(){
		return this.max;
	}
	public void setmax(int max){
		this.max = max;
	}
	public String getfast5_name(){
		return fast5_name;
	}
	public void setfast5_name(String fast5_name){
		this.fast5_name = fast5_name;
	}
}
