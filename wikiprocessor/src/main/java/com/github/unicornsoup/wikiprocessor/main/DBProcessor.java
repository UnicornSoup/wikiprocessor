package com.github.unicornsoup.wikiprocessor.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.log4j.Logger;

/**
 * 
 * @author sjth, anujagarwal464
 * This class extracts data from Wikipedia database tables
 * 
 */
public class DBProcessor {
	static final Logger log = Logger.getLogger(DBProcessor.class);
	Connection con = null;
	String db = "";
	String user = "";
	String password = "";

	DBProcessor(String db, String user, String password) {
		log.debug("constructor start");
		this.db = db;
		this.user = user;
		this.password = password;
		log.debug("constructor end");
	}

	/**
	 * Obtains connection if it is not already there.
	 * 
	 * @return connection object, null for failure
	 */
	private Connection getConnection() {
		log.debug("getConnection start");
		if (con != null)
			return con;
		String url = "jdbc:mysql://localhost:3306/" + db;

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			log.debug("getConnection obtained driver");
			con = DriverManager.getConnection(url, user, password);
			log.debug("getConnection got connection");
		} catch (Exception ex) {
			log.error("getConnection: error:" + ex.getMessage(), ex);
			return null;
		}
		return con;
	}

	/**
	 * This method retrieves pageIds for a specific category from categorylinks
	 * table. It writes the results to a local file.
	 * 
	 * @return true for success, false for failure
	 * @throws FileNotFoundException
	 * @throws SQLException
	 */
	private boolean extractPageIds(String categoryName, String outputFile) {

		// ! Change port accordingly
		Connection con = null;
		Statement stmt = null;
		ArrayList<String> pageIds = new ArrayList<String>();

		try {
			con = getConnection();
			stmt = con.createStatement();
		} catch (Exception ex) {
			log.error(
					"extractPageIds: Cannot open database -- "
							+ "make sure ODBC is configured properly:"
							+ ex.getMessage(), ex);
			return false;
		}

		String query = "select cl_from from categorylinks where cl_to=\""
				+ categoryName + '\"';

		try {
			ResultSet resultSet = stmt.executeQuery(query);
			while (resultSet.next()) {
				pageIds.add(resultSet.getString("cl_from"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

		PrintWriter out;
		try {
			out = new PrintWriter(outputFile);
			for (String pageId : pageIds) {
				out.println(pageId);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		out.close();

		return true;
	}

	/**
	 * This method retrieves page titles for a set of page ids from the page
	 * table. The file which contains a list of page ids is given as input, and
	 * output is where the list of page ids and page titles are written in the
	 * following format: pageid=pagetitle. e.g.: 10324=Jimmy_Wales
	 * 
	 * @return true for success, false for failure
	 */
	private boolean extractPageTitles(String inputFile, String outputFile) {
		Connection con = null;
		Statement stmt = null;
		ArrayList<String> pageIds = new ArrayList<String>();
		ArrayList<String> pageTitles = new ArrayList<String>();

		try {
			con = getConnection();
			stmt = con.createStatement();
		} catch (Exception ex) {
			log.error(
					"extractPageTitles: Cannot open database -- "
							+ "make sure ODBC is configured properly:"
							+ ex.getMessage(), ex);
			return false;
		}

		Scanner scanner;
		try {
			scanner = new Scanner(new File(inputFile));
			while (scanner.hasNext()) {
				pageIds.add(scanner.next());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		scanner.close();

		try {
			for (String pageId : pageIds) {
				String query = "select page_title from page where page_id=\""
						+ pageId + '\"';
				ResultSet resultSet = stmt.executeQuery(query);
				while (resultSet.next()) {
					pageTitles.add(resultSet.getString("page_title"));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

		PrintWriter out;
		try {
			out = new PrintWriter(outputFile);
			for (String pageTitle : pageTitles) {
				out.write(pageTitle);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		out.close();

		return true;
	}

	/**
	 * get page titles for category
	 * 
	 * @return the obvious
	 */
	public boolean getPageTitlesForCategory(String categoryName,
			String tempFile, String outputFile) {
		log.debug("getPageTitlesForCategory start");
		boolean flag = false;
		flag = extractPageIds(categoryName, tempFile);
		if (!flag) {
			log.error("getPageTitlesForCategory: extractPageIds failed");
			return false;
		}
		flag = extractPageTitles(tempFile, outputFile);
		if (!flag) {
			log.error("getPageTitlesForCategory: extractPageTitles failed");
			return false;
		}
		return true;
	}

}
