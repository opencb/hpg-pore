package org.opencb.hpg_pore;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NativePoreSupport {
	public native String getFastqs(byte[] img);
	public native String getInfo(byte[] img);
	public native String getEvents(byte[] img, String src, int min, int max);
	
	public static void loadLibrary(String lib) {
		System.out.println("-----> loading libs..");
		String hostname;
		if(lib == null){
			
			lib = System.getenv("LD_LIBRARY_PATH");
			//lib = "/tmp/libopencb_pore.so";
		}
		File poreLib = new File(lib);
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
