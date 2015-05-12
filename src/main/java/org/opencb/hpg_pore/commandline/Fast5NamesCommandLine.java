package org.opencb.hpg_pore.commandline;

import com.beust.jcommander.Parameter;

public class Fast5NamesCommandLine {
	@Parameter(names = "--in", description = "Input HDFS folder where to find Fast5 files", required = true)
	private String in;

	public String getIn() {
		return in;
	}

	public void setIn(String in) {
		this.in = in;
	}
}
