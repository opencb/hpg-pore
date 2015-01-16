package org.opencb.hadoop_pore;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.opencb.hadoop_pore.StatsWritable.BasicStats;

public class Utils {
	public static long date2seconds(String str_date) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
		Date date = (Date) formatter.parse(str_date); 
		long seconds = date.getTime() / 1000;

		System.out.println(str_date + " = " + seconds);
		return seconds;
	}

	public static void setStatsByInfo(String[] lines, int i, long startTime, BasicStats stats) {
		String v;
		int value;
		
		// seq_length
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.accSeqLength = value;
			stats.minSeqLength = value;
			stats.maxSeqLength = value;
			stats.lengthMap.put(value, 1);
			if (startTime > -1) {
				stats.yieldMap.put(startTime, (long) value);
			}
		}
		
		// A
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numA = value;
		}

		// T
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numT = value;
		}
		// G
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numG = value;
		}
		// C
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numC = value;
		}
		
		// N
		v = lines[i++].split("\t")[1];
		if (!v.isEmpty()) {
			value = Integer.valueOf(v);
			stats.numN = value;
		}		
	}
}
