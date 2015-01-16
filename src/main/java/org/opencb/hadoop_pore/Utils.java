package org.opencb.hadoop_pore;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
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

	public static JFreeChart createHistogram(ArrayList<Double> values, int start, int inc,
			String title, String xLabel, String yLabel) {

		final XYSeries series = new XYSeries("");
		for (int i = 0; i < values.size(); i++) {
	        series.add(i, values.get(i));
		}
		final XYSeriesCollection dataset = new XYSeriesCollection(series);
		
		JFreeChart chart = ChartFactory.createHistogram(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);
		
		return chart;
	}

	public static void saveChart(JFreeChart chart, int width, int height, String fileName) throws IOException {
		File file = new File(fileName);
		ChartUtilities.saveChartAsJPEG(file, chart, width, height);
	}

	public static JFreeChart plotChannelChart(HashMap<Integer, Integer> map,
			String title, String yLabel) {

		int size = 512;
		ArrayList<Double> values = new ArrayList<Double>();
		for(int i = 0; i < size; i++) {
			values.add(0d);			
		}
		for(int key: map.keySet()) {
			values.set(key, (double) map.get(key));
		}
		JFreeChart chart = createHistogram(values, 1, 1, title, "channel", yLabel); 
	        		
        NumberAxis domainAxis = (NumberAxis) chart.getXYPlot().getDomainAxis();            
        domainAxis.setRange(1, 512);
        		
		return chart;
	}
	
	public static JFreeChart plotHistogram(HashMap<Integer, Integer> map, String title, String xLabel, String yLabel) {

		final XYSeries series = new XYSeries("");
		for(int key: map.keySet()) {
			series.add(key, (double) map.get(key));
		}
		final XYSeriesCollection dataset = new XYSeriesCollection(series);
		
		JFreeChart chart = ChartFactory.createHistogram(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);
		
		return chart;
	}

	public static JFreeChart plotCumulativeChart(HashMap<Integer, Integer> map, String title, String xLabel, String yLabel) {	
		Map<Integer, Integer> treeMap = new TreeMap<Integer, Integer>(map);
				
		final XYSeries series = new XYSeries("");
		double acc = 0, start = 0;
		for(int key: treeMap.keySet()) {
			acc += (double) treeMap.get(key);
			series.add((start == 0 ? 0 : key - start), acc);
			if (start == 0) {
				start = key;
			}
		}
				
		final XYSeriesCollection dataset = new XYSeriesCollection(series);
		
		JFreeChart chart = ChartFactory.createXYLineChart(title, xLabel, yLabel, dataset, PlotOrientation.VERTICAL, false, true, false);
		
		return chart;
	}

}
