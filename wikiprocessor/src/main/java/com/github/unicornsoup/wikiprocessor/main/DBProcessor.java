package com.github.unicornsoup.wikiprocessor.main;
/**
 * 
 * @author sp
 * 
 * This class extracts data from  Wikipedia database tables
 *
 */
public class DBProcessor {
	/**
	 * This method retrieves pageIds for a specific category
	 * from categorylinks table. It writes the results to a local file.
	 * 
	 * @return true for success, false for failure
	 */
	public boolean extractPageIds(String categoryName, String outputFile){
		
		return false;//remove this when done
	}
	
	/**
	 * This method retrieves page titles for a set of page ids from the page table.
	 * The file which contains a list of page ids is given as input, and output is
	 * where the list of page ids and page titles are written in the following format:
	 * pageid=pagetitle. e.g.:
	 * 10324=Jimmy_Wales
	 *  @return true for success, false for failure
	 */
	public boolean extractPageTitles(String inputFile, String outputFile){
		
		
		
		
		return false;//remove this when done
	}

}
