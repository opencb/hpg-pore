package org.opencb.hpg_pore.commandline;

import com.beust.jcommander.Parameter;

public class EventsCommandLine {
	  @Parameter(names = "--src", description = "Input folder (or file) where to find Fast5 files", required = true)
	  private String src;
	 
	  @Parameter(names = "--out", description = "Output folder where to save statistics results and charts", required = true)
	  private String out;

	  @Parameter(names = "--hadoop", description = "Run the command on a Hadoop environment")
	  private boolean isHadoop = false;
	  
	  @Parameter(names = "--min", description = "Min start time for extrat the events")
	  private int min;
	  
	  @Parameter(names = "--max", description = "Max time for stop the extrat events")
	  private int max;
	  
	  @Parameter(names = "--lib", description = "Path where library is located")
	  private String lib;

	public String getSrc() {
		return src;
	}

	public void setSrc(String in) {
		this.src = in;
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
	public String getlib(){
		return lib;
	}
	public void setlib(String lib){
		this.lib = lib;
	}
}
