package com.terry.nl.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import org.junit.Test;

import com.terry.nl.NLAnalyze;
import com.terry.nl.NLAnalyzeVO;

public class NLAnalyzeTest {

	@Test
	public void test() {
		
		try {
			NLAnalyze instance = NLAnalyze.getInstance();
			String text="Larry Page, Google's co-founder, once described the 'perfect search engine' as something that 'understands exactly what you mean and gives you back exactly what you want.'";
			//String text="You can bring your Galaxy Note7 on an airplane, just don't turn it on or plug it in";
			NLAnalyzeVO vo = instance.analyze(text);
			
			List<String> nouns = vo.getNouns();
			List<String> adjs = vo.getAdjs();
			
			System.out.println("### NOUNS");
			for(String noun:nouns){
				System.out.println(noun);
			}
			System.out.println("### ADJS");
			for(String adj:adjs){
				System.out.println(adj);
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("API call error");
		} catch (GeneralSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("Security exception");
		}
	}

}
