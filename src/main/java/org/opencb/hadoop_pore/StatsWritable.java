package org.opencb.hadoop_pore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class StatsWritable implements Writable {

	BasicStats sTemplate = new BasicStats();
	BasicStats sComplement = new BasicStats();
	BasicStats s2D = new BasicStats();
	
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

			lengthMap = new HashMap<Integer, Integer>();
			int size = in.readInt();
			for (int i = 0; i < size; i++) {
				lengthMap.put(in.readInt(), in.readInt());
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

				int value = 0;
				for(Object key:stats.lengthMap.keySet()) {
					value = stats.lengthMap.get((Integer) key);
					if (lengthMap.containsKey((Integer) key)) {
						value += lengthMap.get((Integer) key);
					}
					lengthMap.put((Integer) key, value);
				}
			}
		}

		public String toFormat() {
			String res = new String();
			int length = (numA + numT + numG + numC + numN);

			res+= "\n";
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
					res += "Length histogram:\n";
					res += "\tLength\tFrequency\n";
					for(Object key:lengthMap.keySet()) {
						res += "\t" + ((Integer) key) + "\t" + ((Integer) lengthMap.get(key)) + "\n";
					}
				}
			}

			return res;
		}	
	}

	public void readFields(DataInput in) throws IOException {
		sTemplate.readFields(in);
		sComplement.readFields(in);
		s2D.readFields(in);		
	}

	public void write(DataOutput out) throws IOException {
		sTemplate.write(out);
		sComplement.write(out);
		s2D.write(out);		
	}
	
	public String toFormat() {
		String res = new String();
		res += "template\n";
		res += sTemplate.toFormat();
		res += "complement\n";
		res += sComplement.toFormat();
		res += "2D\n";
		res += s2D.toFormat();
		return res;
	}
	
	public String toString() {
		String res = new String();
		res += "template\n";
		res += sTemplate.toString();
		res += "complement\n";
		res += sComplement.toString();
		res += "2D\n";
		res += s2D.toString();
		return res;
	}
}
