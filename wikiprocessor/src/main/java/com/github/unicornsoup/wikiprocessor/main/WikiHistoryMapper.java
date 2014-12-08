package com.github.unicornsoup.wikiprocessor.main;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

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
 * @author sp
 * Reads and processes XML - gets the revision details
 *
 */
public class WikiHistoryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static final Logger log = Logger.getLogger(WikiHistoryMapper.class);
	private final static IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, 
		InterruptedException {
		String inputValue = value.toString();
		try {
			Configuration conf = context.getConfiguration();
			String allPageIds = conf.get("allPageIds");
			String allPageTitles = conf.get("allPageTitles");
			String[] allPageIdsArray = allPageIds.split(HadoopMain.GENERAL_DELIMITER);
			Document document = new SAXBuilder().build(new StringReader(inputValue));
			
			//Element root = document.getRootElement();
			//Iterator iterPages = root.getChildren("page").iterator();
			//while (iterPages.hasNext()){
			//Element page = (Element) iterPages.next();
			Element page = document.getRootElement();
			String pageId =page.getChild("id").getTextTrim();
			//Title is not in "nice format" - is this of any use??!!
			String pageTitle =page.getChild("title").getTextTrim();
			boolean pageIdFound = false;
			
			//Takes O(n^2) - I hate this, but if the # of pages in a category is small, then
			//this should not degrade a lot
			int length = allPageIdsArray.length;
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
				Iterator iterRevisions = page.getChildren("revision").iterator();
				while (iterRevisions.hasNext()){
					Element revision = (Element) iterRevisions.next();
					String userId = revision.getChild("contributor").getChild("id").getTextTrim();
					String revisionId = revision.getChild("id").getTextTrim();				 
					int textLength = revision.getChild("text").getTextTrim().length();		
					String text = revision.getChild("text").getTextTrim();				
	/*					String outputValue = pageId + HadoopMain.GENERAL_DELIMITER 
							+ userId + HadoopMain.GENERAL_DELIMITER 
							+ revisionId + HadoopMain.GENERAL_DELIMITER
							+ textLength + HadoopMain.GENERAL_DELIMITER
							+ text;
						 context.write(new Text(pageId), new Text(outputValue));*/			
					
					context.write(new Text(userId), one);
					
				}//end-while
			
			}
			//}//end-while
		} catch (JDOMException jde) {
			log.error("map: Error while processing xml:" + jde.getMessage(), jde);
		} catch(Exception e){
			log.error("map: Exception:" + e.getMessage(), e);
		}
	
	}


}
