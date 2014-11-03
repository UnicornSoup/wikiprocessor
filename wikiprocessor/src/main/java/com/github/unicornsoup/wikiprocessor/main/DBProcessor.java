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

/**
 * 
 * @author sp, anujagarwal464
 * 
 *         This class extracts data from Wikipedia database tables
 * 
 */
public class DBProcessor {
	/**
	 * This method retrieves pageIds for a specific category from categorylinks
	 * table. It writes the results to a local file.
	 * 
	 * @return true for success, false for failure
	 * @throws FileNotFoundException
	 * @throws SQLException
	 */
	public boolean extractPageIds(String categoryName, String outputFile) {

		// ! Change port accordingly
		String url = "jdbc:mysql://localhost:3306/wikidb";
		Connection con = null;
		Statement stmt = null;
		ArrayList<String> pageIds = new ArrayList<String>();

		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");

			// ! Change password accordingly
			con = DriverManager.getConnection(url, "root", "");
			stmt = con.createStatement();
		} catch (Exception ex) {
			System.out
					.println("Cannot open database -- make sure ODBC is configured properly.");
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
	public boolean extractPageTitles(String inputFile, String outputFile) {

		// ! Change port accordingly
		String url = "jdbc:mysql://localhost:3306/wikidb";
		Connection con = null;
		Statement stmt = null;
		ArrayList<String> pageIds = new ArrayList<String>();
		ArrayList<String> pageTitles = new ArrayList<String>();

		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");

			// ! Change password accordingly
			con = DriverManager.getConnection(url, "root", "");
			stmt = con.createStatement();
		} catch (Exception ex) {
			System.out
					.println("Cannot open database -- make sure ODBC is configured properly.");
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

}
