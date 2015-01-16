package org.opencb.hadoop_pore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class StatsWritable implements Writable {

	public HashMap<Integer, Integer> rChannelMap;
	public HashMap<Integer, Long> yChannelMap;

	public BasicStats sTemplate;
	public BasicStats sComplement;
	public BasicStats s2D;

	public StatsWritable() {
		rChannelMap = new HashMap<Integer, Integer>();
		yChannelMap = new HashMap<Integer, Long>();

		sTemplate = new BasicStats();
		sComplement = new BasicStats();
		s2D = new BasicStats();
	}

	public void update(StatsWritable stats) {
		
		int v1 = 0;
		for(Object key:stats.rChannelMap.keySet()) {
			v1 = stats.rChannelMap.get((Integer) key);
			if (rChannelMap.containsKey((Integer) key)) {
				v1 += rChannelMap.get((Integer) key);
			}
			rChannelMap.put((Integer) key, v1);
		}
		
		long v2 = 0;
		for(Object key:stats.yChannelMap.keySet()) {
			v2 = stats.yChannelMap.get((Integer) key);
			if (yChannelMap.containsKey((Integer) key)) {
				v2 += yChannelMap.get((Integer) key);
			}
			yChannelMap.put((Integer) key, v2);
		}

		sTemplate.update(stats.sTemplate);
		sComplement.update(stats.sComplement);
		s2D.update(stats.s2D);
	}

	public void readFields(DataInput in) throws IOException {
		int size;
		
		rChannelMap = new HashMap<Integer, Integer>();
		size = in.readInt();
		for (int i = 0; i < size; i++) {
			rChannelMap.put(in.readInt(), in.readInt());
		}
		
		yChannelMap = new HashMap<Integer, Long>();
		size = in.readInt();
		for (int i = 0; i < size; i++) {
			yChannelMap.put(in.readInt(), in.readLong());
		}

		sTemplate.readFields(in);
		sComplement.readFields(in);
		s2D.readFields(in);		
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeInt(rChannelMap.size());
		for(Object key:rChannelMap.keySet()) {
			out.writeInt((Integer) key);
			out.writeInt((Integer) rChannelMap.get(key));
		}

		out.writeInt(yChannelMap.size());
		for(Object key:yChannelMap.keySet()) {
			out.writeInt((Integer) key);
			out.writeLong((Long) yChannelMap.get(key));
		}

		sTemplate.write(out);
		sComplement.write(out);
		s2D.write(out);		
	}

	public String toFormat() {
		String res = new String();
		
		res += "\n";
		res += "reads-per-channel\n";
		res += rChannelMap.size() + "\n";
		for(Object key:rChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Integer) rChannelMap.get(key)) + "\n";
		}

		res += "yield-per-channel\n";
		res += yChannelMap.size() + "\n";
		for(Object key:yChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Long) yChannelMap.get(key)) + "\n";
		}
		
		res += "-te\n";
		res += sTemplate.toFormat();
		res += "-co\n";
		res += sComplement.toFormat();
		res += "-2d\n";
		res += s2D.toFormat();
		
		return res;
	}

	public String toString() {
		String res = new String();
		
		res += "reads-per-channel\n";
		res += rChannelMap.size() + "\n";
		for(Object key:rChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Integer) rChannelMap.get(key)) + "\n";
		}

		res += "yield-per-channel\n";
		res += yChannelMap.size() + "\n";
		for(Object key:yChannelMap.keySet()) {
			res += ((Integer) key) + "\t" + ((Long) yChannelMap.get(key)) + "\n";
		}

		res += "-te\n";
		res += sTemplate.toString();
		res += "-co\n";
		res += sComplement.toString();
		res += "-2d\n";
		res += s2D.toString();
		
		return res;
	}

	public class BasicStats { 

		public int numSeqs;
		public int numA;
		public int numT;
		public int numG;
		public int numC;
		public int numN;
		public int minSeqLength;
		public int accSeqLength;
		public int maxSeqLength;
		public HashMap<Integer, Integer> lengthMap;
		public HashMap<Long, Long> yieldMap;

		public BasicStats() {
			numSeqs = 0;
			numA = 0;
			numT = 0;
			numG = 0;
			numC = 0;
			numN = 0;
			minSeqLength = Integer.MAX_VALUE;
			accSeqLength = 0;
			maxSeqLength = 0;
			lengthMap = new HashMap<Integer, Integer>();
			yieldMap = new HashMap<Long, Long>();
		}

		public void write(DataOutput out) throws IOException {
			//int length = 9;
			//out.writeInt(length);
			out.writeInt(numSeqs);
			out.writeInt(numA);
			out.writeInt(numT);
			out.writeInt(numG);
			out.writeInt(numC);
			out.writeInt(numN);
			out.writeInt(minSeqLength);
			out.writeInt(accSeqLength);
			out.writeInt(maxSeqLength);
			
			out.writeInt(lengthMap.size());
			for(Object key:lengthMap.keySet()) {
				out.writeInt((Integer) key);
				out.writeInt((Integer) lengthMap.get(key));
			}
			
			out.writeInt(yieldMap.size());
			for(Object key:yieldMap.keySet()) {
				out.writeLong((Long) key);
				out.writeLong((Long) yieldMap.get(key));
			}
		}

		public void readFields(DataInput in) throws IOException {
			//int length = in.readInt();

			numSeqs = in.readInt();
			numA = in.readInt();
			numT = in.readInt();
			numG = in.readInt();
			numC = in.readInt();
			numN = in.readInt();
			minSeqLength = in.readInt();
			accSeqLength = in.readInt();
			maxSeqLength = in.readInt();

			int size;
			
			lengthMap = new HashMap<Integer, Integer>();
			size = in.readInt();
			for (int i = 0; i < size; i++) {
				lengthMap.put(in.readInt(), in.readInt());
			}
			
			yieldMap = new HashMap<Long, Long>();
			size = in.readInt();
			for (int i = 0; i < size; i++) {
				yieldMap.put(in.readLong(), in.readLong());
			}
		}

		public void update(BasicStats stats) {
			if (stats.accSeqLength > 0) {
				numSeqs++;
				numA += stats.numA;
				numT += stats.numT;
				numG += stats.numG;
				numC += stats.numC;
				numN += stats.numN;
				if (stats.minSeqLength < minSeqLength) minSeqLength = stats.minSeqLength;
				if (stats.maxSeqLength > maxSeqLength) maxSeqLength = stats.maxSeqLength;
				accSeqLength += stats.accSeqLength;

				int v1 = 0;
				for(Object key:stats.lengthMap.keySet()) {
					v1 = stats.lengthMap.get((Integer) key);
					if (lengthMap.containsKey((Integer) key)) {
						v1 += lengthMap.get((Integer) key);
					}
					lengthMap.put((Integer) key, v1);
				}
				
				long v2 = 0;
				for(Object key:stats.yieldMap.keySet()) {
					v2 = stats.yieldMap.get((Long) key);
					if (yieldMap.containsKey((Long) key)) {
						v2 += yieldMap.get((Long) key);
					}
					yieldMap.put((Long) key, v2);
				}
			}
		}

		public String toFormat() {
			String res = new String();
			int length = (numA + numT + numG + numC + numN);

			res += numSeqs + "\n";
			if (numSeqs > 0) {
				res += length + "\n";
				res += minSeqLength + "\n";
				res += maxSeqLength + "\n";
				res += numA + "\n";
				res += numT + "\n";
				res += numG + "\n";
				res += numC + "\n";
				res += numN + "\n";

				res += lengthMap.size() + "\n";
				for(Object key:lengthMap.keySet()) {
					res += ((Integer) key) + "\t" + ((Integer) lengthMap.get(key)) + "\n";
				}
				
				res += yieldMap.size() + "\n";
				for(Object key:yieldMap.keySet()) {
					res += ((Long) key) + "\t" + ((Long) yieldMap.get(key)) + "\n";
				}
			}

			return res;
		}	


		@Override
		public String toString() {
			String res = new String();
			int length = (numA + numT + numG + numC + numN);

			res += "Num. seqs: " + numSeqs + "\n";
			if (numSeqs > 0) {
				res += "Num. total nucleotides: " + length + " (" + accSeqLength + ")\n";
				res += "Seq. length (min, avg, max) = (" + minSeqLength + ", " + String.format("%.2f", 1.0f * accSeqLength / numSeqs) + ", " + maxSeqLength + ")\n";

				res += "A: " + numA + " " + String.format("%.2f", 100.0f * numA / length) + "%, ";
				res += "T: " + numT + " " + String.format("%.2f", 100.0f * numT / length) + "%, ";
				res += "G: " + numG + " " + String.format("%.2f", 100.0f * numG / length) + "%, ";
				res += "C: " + numC + " " + String.format("%.2f", 100.0f * numC / length) + "%, ";
				res += "N: " + numN + " " + String.format("%.2f", 100.0f * numN / length) + "%\n";

				res += "GC: " + String.format("%.2f", 100.0f * (numG + numC) / length) + "% \n";

				if (numSeqs > 1) {
					res += "Read length histogram:\n";
					res += "\tLength\tFrequency\n";
					for(Object key:lengthMap.keySet()) {
						res += "\t" + ((Integer) key) + "\t" + ((Integer) lengthMap.get(key)) + "\n";
					}
					
					res += "Cummulative yield:\n";
					res += "\tTime (in seconds)\tNum. nt\n";
					for(Object key:yieldMap.keySet()) {
						res += "\t" + ((Long) key) + "\t" + ((Long) yieldMap.get(key)) + "\n";
					}
				}
			}

			return res;
		}	
	}
}
