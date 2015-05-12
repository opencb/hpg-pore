package org.opencb.hpg_pore.commandline;

import com.beust.jcommander.Parameter;

public class SignalCommandLine {

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

	@Parameter(names = "--fast5name", description = "The old name of the fast5  the user want to use ")
	private String fast5name;

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
	public void setMin(int min){
		this.min = min;
	}
	public int getMin (){
		return this.min;
	}
	public int getMax(){
		return this.max;
	}
	public void setMax(int max){
		this.max = max;
	}
	public String getFast5name(){
		return fast5name;
	}
	public void setFast5name(String fast5name){
		this.fast5name = fast5name;
	}
}
