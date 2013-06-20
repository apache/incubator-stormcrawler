package com.digitalpebble.storm.crawler.filtering;

/** Unlike Nutch, URLFilters can normalise the URLs as well as filtering them **/ 
public interface URLFilter {
	
	/** Returns null if the URL is to be removed or a normalised representation which can correspond to the input URL **/
	public String filter (String URL);

}
