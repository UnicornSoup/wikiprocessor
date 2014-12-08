package com.github.unicornsoup.wikiprocessor.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Main {
	
	static final Logger log = Logger.getLogger(Main.class);

	/**
	 * Gets system property values. If abort flag is true, exits the program if property is
	 * not found. Otherwise, logs the error.
	 * 
	 * @return property value
	 */
	public static String getProperty(String propertyName, boolean abort){
		String property = System.getProperty(propertyName);
		if (property == null){
			log.error("getProperty: error: property " + propertyName + " not found!");
			if (abort) System.exit(-1);
		}
		return property;
	}

	
	/**
	 * 
	 * The main method does one of two things, depending on the argument passed in for 
	 * "process".
	 * If process = "pages":
	 * This method will expect to be passed the following arguments:
	 * 		log4jprops - log4j property file path
	 * 		db - database name
	 * 		user - db user
	 * 		password - db password
	 * 		categoryName - name of Wikipedia category
	 * 		tempFile - intermediate output file
	 * 		outputFile - final output
	 * It will extract the pages from a mySQL database and store
	 * the output in an output file in the format pageId=pageTitle.
	 * If process = "history":
	 * This method will expect path to an input file (inputFile) as the second argument
	 * It will also expect the directory where history files are 
	 * stored (historyDirectory) as the third argument.
	 * It will search for the page titles within the history files.
	 * For each page title, it will tabulate the contributions done by registered users
	 * and anonymous users.
	 * 
	 * 
	 * @param args
	 */
	public static void main(String args[]){
		String log4jFilePath = getProperty("log4jprops", true);
		System.out.println("log4jFilePath=" + log4jFilePath);
		PropertyConfigurator.configure(log4jFilePath);
		
		String db = getProperty("db", true);
		String user = getProperty("user", true);
		String password = getProperty("password", true);
		
		String categoryName = getProperty("categoryName", true);
		String outputFile = getProperty("outputFile", true);
		String tempFile = getProperty("tempFile", true);

		DBProcessor dbProcessor = new DBProcessor(db, user, password);
		dbProcessor.getPageTitlesForCategory(categoryName, tempFile, outputFile);
		
		log.debug("Main: end");
		
	}

}
