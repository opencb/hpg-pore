package org.opencb.hpg_pore;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NativePoreSupport {
	public native String getFastqs(byte[] img);
	public native String getInfo(byte[] img);
	public native String getEvents(byte[] img, String src, int min, int max);
	
	public static void loadLibrary() {
		System.out.println("-----> loading libs..");
		String hostname;
		File poreLib = new File("/tmp/libopencb_pore.so");
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			hostname = new String("no-name");
		}
		if (poreLib.exists()) {
			System.out.println("*********** " + poreLib.getAbsolutePath() + " exists (" + hostname + ")");
		} else {
			System.out.println("*********** " + poreLib.getAbsolutePath() + " does NOT exist (" + hostname + ")");				
		}
		System.load(poreLib.getAbsolutePath());		
	}
}
