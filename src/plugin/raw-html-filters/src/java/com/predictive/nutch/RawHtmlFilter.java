package com.predictive.nutch;

import java.io.*;
import java.util.*;

import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.nutch.indexer.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.crawl.*;
import org.apache.nutch.metadata.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.util.NodeWalker;

import org.slf4j.*;

import org.w3c.dom.*;
import org.w3c.dom.html.*;
import org.jsoup.Jsoup;


public class RawHtmlFilter implements HtmlParseFilter {
    public static Logger LOG = LoggerFactory.getLogger(IndexingJob.class);

    private DocumentFragment doc;
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
        this.doc = doc;
        try {
            Metadata metadata = parseResult.get(content.getUrl()).getData().getParseMeta();
            String contentType = content.getMetadata().get(HttpHeaders.CONTENT_TYPE);
            System.out.println(contentType);
            if(contentType != null) {
                String[] parts = contentType.split("charset=");
                if(parts.length > 1) {
                    contentType = parts[1];
                } else {
                    contentType = null;
                }
            }
            if(contentType == null) {
                contentType = "utf-8";
            }

            //Raw data
            byte[] rawContent = content.getContent();
            String str = new String(rawContent, contentType.toUpperCase());
            metadata.add("metatag.rawcontent", str);

            String[] tags = new String[]{
                "h1",
                "h2",
                "h3",
                "h4",
                "table"
            };

            //Generic tags
            for(String tag : tags) {
                List<String[]> tagValues = getElement(tag);
                for(String[] elVals : tagValues) {
                    String rawTag = elVals[0];
                    String value = elVals[1];
                    metadata.add("metatag." + tag + ".raw", rawTag);
                    metadata.add("metatag." + tag + ".value", value);
                }
            }

            //Anchor tags
            for(String[] aVals : getAnchorElement()) {
                String rawTag = aVals[0];
                String value  = aVals[1];
                String link   = aVals[2];
                metadata.add("metatag.a.raw", rawTag);
                metadata.add("metatag.a.value", value);
                metadata.add("metatag.a.link", link);
            }

            for(String[] aVals : getImageElement()) {
                String rawTag = aVals[0];
                String image  = aVals[1];
                metadata.add("metatag.img.raw", rawTag);
                metadata.add("metatag.img.image", image);
            }

            this.doc = null;

            return parseResult;
        } catch(java.io.UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected List<String[]> getImageElement() {
        String element = "img";
        List<String[]> headings = new ArrayList<String[]>();
        NodeWalker walker = new NodeWalker(doc);

        while (walker.hasNext()) {
            Node currentNode = walker.nextNode();

            if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                if (element.equalsIgnoreCase(currentNode.getNodeName())) {
                    if(currentNode instanceof HTMLImageElement) {
                        HTMLImageElement image = (HTMLImageElement)currentNode;
                        String stringNode = xmlToString(currentNode);
                        String[] val = new String[]{stringNode, image.getSrc()};
                        headings.add(val);
                    }
                }
            }
        }

        return headings;
    }

    protected List<String[]> getAnchorElement() {
        String element = "a";
        List<String[]> headings = new ArrayList<String[]>();
        NodeWalker walker = new NodeWalker(doc);

        while (walker.hasNext()) {
            Node currentNode = walker.nextNode();

            if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                if (element.equalsIgnoreCase(currentNode.getNodeName())) {
                    if(currentNode instanceof HTMLAnchorElement) {
                        HTMLAnchorElement anchor = (HTMLAnchorElement)currentNode;
                        String nodeValue = getNodeValue(currentNode);
                        if(nodeValue != null && nodeValue.trim().length() > 0) {
                            String stringNode = xmlToString(currentNode);
                            String[] val = new String[]{stringNode, nodeValue,anchor.getHref()};
                            headings.add(val);
                        }
                    }
                }
            }
        }

        return headings;
    }

    /**
     * Finds the specified element and returns its value
     */
    protected List<String[]> getElement(String element) {
        List<String[]> headings = new ArrayList<String[]>();
        NodeWalker walker = new NodeWalker(doc);

        while (walker.hasNext()) {
            Node currentNode = walker.nextNode();

            if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                if (element.equalsIgnoreCase(currentNode.getNodeName())) {
                    String nodeValue = getNodeValue(currentNode);
                    if(nodeValue != null && nodeValue.trim().length() > 0) {
                        String stringNode = xmlToString(currentNode);
                        String[] val = new String[]{stringNode, nodeValue};
                        headings.add(val);
                    }
                }
            }
        }

        return headings;
    }

    /**
     * Returns the text value of the specified Node and child nodes
     */
    protected static String getNodeValue(Node node) {
        StringBuilder buffer = new StringBuilder();

        NodeList children = node.getChildNodes();

        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i).getNodeType() == Node.TEXT_NODE) {
                buffer.append(children.item(i).getNodeValue());
            } else {
                buffer.append(getNodeValue(children.item(i)));
            }
        }

        return buffer.toString();
    }

    public static String xmlToString(Node node) {
        try {
            Source source = new DOMSource(node);
            StringWriter stringWriter = new StringWriter();
            Result result = new StreamResult(stringWriter);
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.transform(source, result);
            return stringWriter.getBuffer().toString();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        }
        return null;
    }

}
