package com.predictive.nutch;

import java.io.*;

import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.nutch.indexer.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.crawl.*;

import org.slf4j.*;

public class WhitelistFilter implements IndexingFilter {

    public static final String INDEXFILTER_WHITELIST_FILE = "indexfilter.whitelist.file";
    public static Logger LOG = LoggerFactory.getLogger(IndexingJob.class);

    private Configuration conf;
    private Set<String> whitelist;

    public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
        throws IndexingException {

        NutchField content = doc.getField("content");

        for(Object value : content.getValues()) {
            String strValue = (String)value;
            if(strValue != null) {
                String[] parts = strValue.toLowerCase().split(" ");
                for(String part : parts) {
                    if(whitelist.contains(part)) {
                        LOG.debug("Added {} to the collection", url);
                        return doc;
                    }
                }
            }
        }

        return null;
    }

    private Set<String> readWhitelist(Reader reader)
        throws IOException {
        Set<String> result = new HashSet<>();

        BufferedReader in = new BufferedReader(reader);
        String line;

        while((line=in.readLine())!=null) {
            result.add(line.toLowerCase());
        }

        return result;
    }

    protected Reader getListReader(Configuration conf) throws IOException {
        String fileList = conf.get(INDEXFILTER_WHITELIST_FILE);
        return conf.getConfResourceAsReader(fileList);
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    
        try {
            whitelist = readWhitelist(getListReader(conf));
        } catch(IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
