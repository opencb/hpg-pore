package org.opencb.hadoop_pore;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import javax.imageio.ImageIO;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

public class HistogramGraph {
	private Graph graph = null;
	public HistogramGraph(Map<Integer, Integer> hist) throws IOException {
		graph = new Graph(hist);        
	}
 
	public void save(String fileName) throws IOException {
		System.out.println("(width, height) = (" + graph.getWidth() + ", " + graph.getHeight() + ")");
		BufferedImage bi = new BufferedImage(graph.getWidth(), graph.getHeight(), BufferedImage.TYPE_INT_ARGB);
		graph.paint(bi.getGraphics());
		
		File outputfile = new File(fileName);
	    ImageIO.write(bi, "png", outputfile);		
	}
	
	protected class Graph extends JPanel {

		protected static final int MIN_BAR_WIDTH = 2;
		private Map<Integer, Integer> mapHistory;

		public Graph(Map<Integer, Integer> mapHistory) {
			this.mapHistory = mapHistory;
			
			int width = 0;
			for (Integer key : mapHistory.keySet()) {
				width = Math.max(width, key);
			}

			width = (width * MIN_BAR_WIDTH) + 11;
			if (width > 1024) width = 1024;
			
			//int width = (mapHistory.size() * MIN_BAR_WIDTH) + 11;
			Dimension minSize = new Dimension(width, 128);
			Dimension prefSize = new Dimension(width, 256);
			setMinimumSize(minSize);
			setPreferredSize(prefSize);
			setSize(prefSize);
		}

		@Override
		protected void paintComponent(Graphics g) {
			super.paintComponent(g);
			if (mapHistory != null) {
				int xOffset = 5;
				int yOffset = 5;
				int width = getWidth() - 1 - (xOffset * 2);
				int height = getHeight() - 1 - (yOffset * 2);
				Graphics2D g2d = (Graphics2D) g.create();
				g2d.setColor(Color.DARK_GRAY);
				g2d.drawRect(xOffset, yOffset, width, height);
				int barWidth = MIN_BAR_WIDTH;
				//int barWidth = Math.max(MIN_BAR_WIDTH,
				//		(int) Math.floor((float) width
				//				/ (float) mapHistory.size()));
				//System.out.println("width = " + width + "; size = "
				//		+ mapHistory.size() + "; barWidth = " + barWidth);
				int maxValueX = 0;
				int maxValueY = 0;
				for (Integer key : mapHistory.keySet()) {
					int value = mapHistory.get(key);
					maxValueX = Math.max(maxValueX,key);
					maxValueY = Math.max(maxValueY, value);
				}
				//int xPos = xOffset;
				for (Integer key : mapHistory.keySet()) {
					int value = mapHistory.get(key);
					int barHeight = Math.round(((float) value
							/ (float) maxValueY) * height);
					g2d.setColor(new Color(key % 255, key % 255, key % 255));
					int yPos = height + yOffset - barHeight;
					int xPos = key * maxValueX / width;
					//Rectangle bar = new Rectangle(xPos, yPos, barWidth, barHeight);
					Rectangle2D bar = new Rectangle2D.Float(
							xPos, yPos, barWidth, barHeight);
					g2d.fill(bar);
					g2d.setColor(Color.DARK_GRAY);
					g2d.draw(bar);
					xPos += barWidth;
				}
				g2d.dispose();
			}
		}
	}
}
