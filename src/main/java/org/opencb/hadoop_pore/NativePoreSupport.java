package org.opencb.hadoop_pore;

public class NativePoreSupport {
	public native String getFastqs(byte[] img);
	public native String getInfo(byte[] img);
}
