package com.github.unicornsoup.wikiprocessor.main;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

public class DBProcessorTest {
	static {
		System.setProperty(
				"log4jprops",
				"/home/anuj/Desktop/wikiprocessor/wikiprocessor/src/test/resources/log4j.properties");
		System.setProperty("db", "wikidb");
		System.setProperty("user", "root");
		System.setProperty("password", "");
		System.setProperty("categoryName", "Unprintworthy_redirects");
		System.setProperty("tempFile", "tmpIds");
		System.setProperty("outputFile", "outputFile");
	}

	@Test
	public void testGetPageTitlesForCategory() {
		String log4jFilePath = System.getProperty("log4jprops");
		System.out.println("log4jFilePath=" + log4jFilePath);
		PropertyConfigurator.configure(log4jFilePath);

		String db = System.getProperty("db");
		String user = System.getProperty("user");
		String password = System.getProperty("password");

		String categoryName = "Unprintworthy_redirects";
		String tempFile = "tmpIds";
		String outputFile = "outputFile";

		DBProcessor dbProcessor = new DBProcessor(db, user, password);
		Assert.assertEquals(true, dbProcessor.getPageTitlesForCategory(
				categoryName, tempFile, outputFile));
	}
}
