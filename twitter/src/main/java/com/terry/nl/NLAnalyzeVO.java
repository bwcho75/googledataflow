package com.terry.nl;

import java.util.ArrayList;
import java.util.List;

public class NLAnalyzeVO {
	List<String> nouns = new ArrayList<String>();
	List<String> adjs = new ArrayList<String>();
	List<String> emoticons = new ArrayList<String>();
	float sentimental;

	public List<String> getNouns() {
		return nouns;
	}

	public List<String> getAdjs() {
		return adjs;
	}

	public List<String> getEmoticons() {
		return emoticons;
	}

	public float getSentimental() {
		return sentimental;
	}

	public void setSentimental(float sentimental) {
		this.sentimental = sentimental;
	}
	
	public void addNouns(String n){
		nouns.add(n);
	}
	
	public void addAdj(String a){
		adjs.add(a);
	}
	
	public void addEmoticons(String e){
		emoticons.add(e);
	}
}
