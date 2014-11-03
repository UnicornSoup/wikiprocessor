package com.github.unicornsoup.wikiprocessor.main;

import org.junit.Assert;
import org.junit.Test;

public class DBProcessorTest {
	
	@Test
	public void testExtractPageIds(){
		String categoryName = "Unprintworthy_redirects"; 
		String outputFile = "tmpIds";
		DBProcessor dbProcessor = new DBProcessor();
		Assert.assertEquals(true, dbProcessor.extractPageIds(categoryName, outputFile));
	}
	
	@Test
	public void testExtractPageTitles(){
		String inputFile = "tmpIds"; 
		String outputFile = "tmpTitles";
		DBProcessor dbProcessor = new DBProcessor();
		Assert.assertEquals(true, dbProcessor.extractPageTitles(inputFile, outputFile));
	}	

}
