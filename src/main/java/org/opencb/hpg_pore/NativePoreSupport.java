package org.opencb.hpg_pore;

public class NativePoreSupport {
	public static final String LIB_NAME = "hpgpore";
	public native String getFastqs(byte[] img);
	public native String getInfo(byte[] img);
	public native String getEvents(byte[] img, String src, int min, int max);
	
	public static void loadLibrary() {
		try {
			System.loadLibrary(LIB_NAME);
		} catch (UnsatisfiedLinkError e) {
			String property = System.getProperty("java.library.path");
			System.out.println("java.library.path = " + property);
			System.out.println("Error loading dynamic library " + LIB_NAME +
					", check your library is saved in " + property);
			System.out.println("Set the environment variable LD_LIBRARY_PATH");
			System.exit(-1);
		}
	}
}
