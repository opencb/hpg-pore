package org.opencb.hpg_pore.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ParamsforDraw{
	public int cumul_qual;
	public int frequency;
	public int numA;
	public int numT;
	public int numC;
	public int numG;
	public int numN;

	public ParamsforDraw() {
		cumul_qual = 0;
		frequency = 0;
		numA = 0;
		numT = 0;
		numC = 0;
		numG = 0;
		numN = 0;
	}
	public void updateParams(ParamsforDraw p){
		this.cumul_qual += p.cumul_qual;
		this.frequency += p.frequency;
		this.numA += p.numA;
		this.numT += p.numT;
		this.numC += p.numC;
		this.numG += p.numG;
		this.numN += p.numN;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(cumul_qual);
		out.writeInt(frequency);
		out.writeInt(numA);
		out.writeInt(numT);
		out.writeInt(numG);
		out.writeInt(numC);
		out.writeInt(numN);
		
	}
	public void readFields(DataInput in) throws IOException {
		//int length = in.readInt();

		cumul_qual = in.readInt();
		frequency = in.readInt();
		numA = in.readInt();
		numT = in.readInt();
		numG = in.readInt();
		numC = in.readInt();
		numN = in.readInt();
		
	}
}
