package pageRank_assignment3;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;


public class GraphMakerMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private static Pattern namePattern;
	private static Pattern linkPattern;
	
	/* 
	 * Defining the pattern that is to be used by the pattern matcher to match the pagename
	 * and the links.
	 */
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}
	
	/**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     *
     * Specifically, this method should take Text objects in the form
     * 		 "<html> </html>"
     * and store a new key containing the page and page rank delimited by ~~ and value containing Adjacency list:
     * 		  "pageName~0A0"	"[adjacency list]"
     */
	// Override the map method of the Mapper in the GraphBuilder
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String absFileName = value.toString();
		boolean errorOccured = false;

		try {
			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			
			// Parser fills this list with linked page names.
			List<Text> linkPageNames = new LinkedList<Text>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
			String line = absFileName;
			
			// Each line formatted as (Wiki-page-name:Wiki-page-html).
			int delimLoc = line.indexOf(':');
			String pageName = line.substring(0, delimLoc);
			String html = line.substring(delimLoc + 1);
			Matcher matcher = namePattern.matcher(pageName);
			if (!matcher.find())
				errorOccured = true;

			// Skip this html file, name contains (~).
			if (!errorOccured) {
				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					xmlReader.parse(new InputSource(new StringReader(html)));
					// Discard ill-formatted pages.
					} catch (Exception e) {
						errorOccured = true;
					}
					
					// If no error occurred then get the Adjacency List.
					if(!errorOccured) {
//						context.getCounter(PageValue.page.PAGE_COUNT).increment(1);
//						System.out.println(pageName+"  --   "+ linkPageNames);
//						System.out.println("hello");
//						StringBuilder str = new StringBuilder();
//							for (Text s : linkPageNames){
//								System.out.println();
//							}
//								str.append(s.toString()).append("~");
						context.write(new Text(pageName.trim()), new Text("s@!"+linkPageNames.toString()));
					}
				}
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}	
	
	
	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		
		/** List of linked pages; filled by parser. */
		private List<Text> linkPageNames;
		
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<Text> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0)
				// Beginning of bodyContent div element.
				count = 1;
			else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				
				if (link == null)
					return;
				
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				
				if (matcher.find())
					linkPageNames.add(new Text(matcher.group(1)));
			} 
			else if (count > 0)
				// Other element inside bodyContent div.
				count++;
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			
			super.endElement(uri, localName, qName);
			if (count > 0)
				// End of element inside bodyContent div.
				count--;
		}
	}
}
