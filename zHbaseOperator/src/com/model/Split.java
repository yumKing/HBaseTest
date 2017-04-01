package com.model;

import java.util.ArrayList;
import java.util.List;

public class Split {

	private  String start;
	private  String end;
	
	
	public static Split[] split(String start,String end,int splitNum){
//		Split[] splits = new Split[splitNum];
		List<Split> splits = new ArrayList<>();
		
		for(int i = 0;i<splitNum;i++){
			Split sp = new Split();
			sp.start = start;
			sp.end = end;
			splits.add(sp);
		}
		
		return splits.toArray(new Split[splitNum]);
	}


	public String getStart() {
		return start;
	}


	public void setStart(String start) {
		this.start = start;
	}


	public String getEnd() {
		return end;
	}


	public void setEnd(String end) {
		this.end = end;
	}
	
}
