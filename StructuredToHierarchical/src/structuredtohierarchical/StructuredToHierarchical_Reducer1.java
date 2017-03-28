/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package structuredtohierarchical;

import org.w3c.dom.Document;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Element;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;
import org.w3c.dom.NamedNodeMap;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerException;
import org.xml.sax.InputSource;
import javax.xml.transform.dom.DOMSource;
import org.xml.sax.SAXException;

/**
 *
 * @author mansijain
 */
public class StructuredToHierarchical_Reducer1 extends Reducer<Object, Text, Text, NullWritable> {

    private ArrayList<String> tags = new ArrayList<>();
    private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    private String movie;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, ParserConfigurationException, TransformerException, SAXException {
        movie = null;
        tags.clear();
        for (Text t : values) {
            if (t.charAt(0) == 'm') {
                movie = t.toString().trim();
            } else {
                tags.add(t.toString().trim());
            }
        }

        if (movie != null) {
            String movieWithTagChildren = nestElements(movie, tags);

            context.write(new Text(movieWithTagChildren), NullWritable.get());
        }
    }

    private String nestElements(String movie, List<String> tags) throws ParserConfigurationException, TransformerException, IOException, SAXException {
        DocumentBuilder bldr = dbf.newDocumentBuilder();
        Document doc = bldr.newDocument();

        Element movieEl = getXmlElementFromString(movie);
        Element toAddMovieEl = doc.createElement("movie");

        // Copy the attributes of the original post element to the new one
        copyAttributesToElement(movieEl.getAttributes(), toAddMovieEl);

        // For each comment, copy it to the "post" node
        for (String tagXml : tags) {
            Element tagEl = getXmlElementFromString(tagXml);
            Element toAddTagEl = doc.createElement("tags");

            // Copy the attributes of the original comment element to
            // the new one
            copyAttributesToElement(tagEl.getAttributes(),
                    toAddTagEl);

            // Add the copied comment to the post element
            toAddMovieEl.appendChild(toAddTagEl);
        }

        // Add the post element to the document
        doc.appendChild(toAddMovieEl);

        // Transform the document into a String of XML and return
        return transformDocumentToString(doc);
    }

    private Element getXmlElementFromString(String xml) throws ParserConfigurationException, SAXException, IOException {
        // Create a new document builder
        DocumentBuilder bldr = dbf.newDocumentBuilder();

        return bldr.parse(new InputSource(new StringReader(xml)))
                .getDocumentElement();
    }

    private void copyAttributesToElement(NamedNodeMap attributes,
            Element element) {

        // For each attribute, copy it to the element
        for (int i = 0; i < attributes.getLength(); ++i) {
            Attr toCopy = (Attr) attributes.item(i);
            element.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }

    private String transformDocumentToString(Document doc) throws TransformerException {

        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
                "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(
                writer));
        // Replace all new line characters with an empty string to have
        // one record per line.
        return writer.getBuffer().toString().replaceAll("\n|\r", "");
    }

}
