package com.github.unicornsoup.wikiprocessor.main;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 * 
 * @author sjth 
 * Reads and processes XML - gets the revision details
 * 
 */
public class WikiHistoryMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	static final Logger log = Logger.getLogger(WikiHistoryMapper.class);
	private final static IntWritable one = new IntWritable(1);
<<<<<<< Updated upstream

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
=======
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, 
		InterruptedException {
>>>>>>> Stashed changes
		String inputValue = value.toString();
		log.debug("map: inputValueINMAP=" + inputValue + "::END");
		try {
			Configuration conf = context.getConfiguration();

			String allPageIds = conf.get("allPageIds");
			String allPageTitles = conf.get("allPageTitles");
<<<<<<< Updated upstream
			String[] allPageIdsArray = allPageIds
					.split(HadoopMain.GENERAL_DELIMITER);

			Document document = new SAXBuilder().build(inputValue);
			Element page = document.getRootElement();
			String pageId = page.getChild("id").getTextTrim();
			boolean pageIdFound = false;

			// Takes O(n^2) - I hate this, but if the # of pages in a category
			// is small, then this should not degrade a lot
			int length = allPageIdsArray.length;
			for (int i = 0; i < length; i++) {
				if (pageId.equals(allPageIdsArray[i])) {
					pageIdFound = true;
					break;
				}
			}// end-for

			if (pageIdFound) {

				// This is the all-important section dealing with revisions
				// Each time a user's id appears, it is added to the context to
				// be passed to reducer
				Iterator iterRevisions = page.getChildren("revision")
						.iterator();
				while (iterRevisions.hasNext()) {
					Element revision = (Element) iterRevisions.next();
					String userId = revision.getChild("contributor")
							.getChild("id").getTextTrim();
					String revisionId = revision.getChild("id").getTextTrim();
					int textLength = revision.getChild("text").getTextTrim()
							.length();
					String text = revision.getChild("text").getTextTrim();
					/*
					 * String outputValue = pageId +
					 * HadoopMain.GENERAL_DELIMITER + userId +
					 * HadoopMain.GENERAL_DELIMITER + revisionId +
					 * HadoopMain.GENERAL_DELIMITER + textLength +
					 * HadoopMain.GENERAL_DELIMITER + text; context.write(new
					 * Text(pageId), new Text(outputValue));
					 */

					context.write(new Text(userId), one);

				}// end-while

=======
			String[] allPageIdsArray = allPageIds.split(HadoopMain.GENERAL_DELIMITER);
			log.debug("map: starting to read xml");
			Document document = new SAXBuilder().build(new StringReader(inputValue));
			log.debug("map: finished reading xml, getting page");
			Element page = document.getRootElement();
			log.debug("map: obtained page");
			String pageId =page.getChild("id").getTextTrim();
			log.debug("map: obtained pageId");
			//Title is not in "nice format" - is this of any use??!!
			String pageTitle =page.getChild("title").getTextTrim();
			log.debug("map: obtained pageTitle");
			boolean pageIdFound = false;
			//Takes O(n^2) - I hate this, but if the # of pages in a category is small, then
			//this should not degrade a lot
			int length = allPageIdsArray.length;
			log.debug("map: allPageIdsArray.length=" + length);
			for (int i = 0; i < length; i++){
				if (pageId.equals(allPageIdsArray[i])){
					pageIdFound = true;
					break;
				}
			}//end-for
			
			if (pageIdFound){
			
				//This is the all-important section dealing with revisions
				//Each time a user's id appears, it is added to the context to be
				//passed to reducer
				log.debug("map: found pageId");
				Iterator iterRevisions = page.getChildren("revision").iterator();
				while (iterRevisions.hasNext()){
					Element revision = (Element) iterRevisions.next();
					log.debug("map: revision=" + revision);
					log.debug("map: revisioncontentsize=" + revision.getContentSize());
					Element contributor = revision.getChild("contributor");
					log.debug("map: contributor=" + contributor);
					String userId = (contributor.getChild("id") == null)?contributor.getChild("ip").getTextTrim()
							:contributor.getChild("id").getTextTrim();
					log.debug("map: userId=" + userId);
					String revisionId = revision.getChild("id").getTextTrim();				 
					int textLength = revision.getChild("text").getTextTrim().length();		
					String text = revision.getChild("text").getTextTrim();				
					context.write(new Text(userId), one);
				}//end-while
			
>>>>>>> Stashed changes
			}
			// end-while
		} catch (JDOMException jde) {
			log.error("map: Error while processing xml:" + jde.getMessage(),
					jde);
		} catch (Exception e) {
			log.error("map: Exception:" + e.getMessage(), e);
		}

	}

}
