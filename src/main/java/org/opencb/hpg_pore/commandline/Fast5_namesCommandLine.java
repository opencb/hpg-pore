package org.opencb.hpg_pore.commandline;

import com.beust.jcommander.Parameter;

public class Fast5_namesCommandLine {
	 @Parameter(names = "--in", description = "Input folder (or file) where to find Fast5 files", required = true)
	  private String in;
	 
	  @Parameter(names = "--out", description = "Output folder where to save statistics results and charts", required = true)
	  private String out;

	  
	  @Parameter(names = "--lib", description = "Path where library is located")
	  private String lib;

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

	
	public String getlib(){
		return lib;
	}
	public void setlib(String lib){
		this.lib = lib;
	}
}
