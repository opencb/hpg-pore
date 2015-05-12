package org.opencb.hpg_pore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;

public class NativePoreSupport {
	public static final String LIB_NAME = "hpgpore";
	public static final String LIB_FULLNAME = "libhpgpore.so";

	public native String getFastqs(byte[] img);
	public native String getInfo(byte[] img);
	public native String getEvents(byte[] img, String src, int min, int max);

	public static void checkLibrary(FileSystem fs) {
		try {
			System.out.println("Checking library " + LIB_FULLNAME + " in HDFS...");
			if (!fs.exists(new Path(LIB_FULLNAME))) {
				System.out.println("Warning: The library " + LIB_FULLNAME + " does not exist in HDFS. It will be copied there.");
				fs.copyFromLocalFile(new Path("./" + LIB_FULLNAME), new Path(LIB_FULLNAME));
				if (fs.exists(new Path(LIB_FULLNAME))) {
					System.out.println("Copy successfull!");
				} else {
					System.out.println("Copy failed. Be sure you have the library " + LIB_FULLNAME + " in your local directory");
					System.exit(-1);
				}
			} else {
				System.out.println("Checking successfull!");
			}
		} catch (FileNotFoundException e) {
			System.out.println("Copy failed. Be sure you have the library " + LIB_FULLNAME + " in your local directory");
			System.exit(-1);
		} catch (Exception e) {
			System.out.println("Error checking library " + LIB_FULLNAME + ": " + e.getMessage());
			System.exit(-1);
		}
	}

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
