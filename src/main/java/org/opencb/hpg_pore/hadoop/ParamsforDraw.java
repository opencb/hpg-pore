package org.opencb.hpg_pore.hadoop;

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
}
