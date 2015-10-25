package _other.xml;

import org.apache.commons.lang.StringEscapeUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class XmlStringParser {

    Document doc;

    public XmlStringParser( String xml ) {

        String escapedXml = getEscapedXml( xml );

        try {

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();

            InputSource input = new InputSource();

            try {

                input.setCharacterStream( new StringReader( escapedXml ) );
                doc = builder.parse( input );

            }
            // If for some reason, the xml still can't be parsed properly, or the InputSource is not functioning properly.
            catch ( Exception e) {

                input.setCharacterStream( new StringReader( "<tag></tag>" ) );

                try {
                    doc = builder.parse( input );

                } catch ( Exception e2 ) {

                    // Something has gone horribly wrong
                    System.exit( -1 );
                }
            }

        } catch ( ParserConfigurationException e ) {

            e.printStackTrace();
        }

    }

    /**
     * Some characters in the XML need to escaped in order to prevent
     * {@link jdk.internal.org.xml.sax.SAXParseException} with message "The entity *entity* was referenced, but not declared".
     *
     * Also makes it possible to implement this as a robust XmlStringParser in other projects, in order to ensure that the input is
     * validated.
     *
     * @param xml The original xml input, with possible invalid characters
     *
     * @return A version of the orginial xml input, where all invalid characters have been escaped
     */
    private String getEscapedXml( String xml ) {

        // To indicate whether the start and end of an invalid character has been found
        boolean start = false, end = false;

        // The String which is going to be returned as the finished, escaped, version of the xml
        String escapedXml = "";

        // A container for the invalid character, to be escaped
        String temp = "";

        for ( char character : xml.toCharArray() ) {

            // Start of an invalid character
            if ( character == '&' ) start = true;

            if ( start ) {

                temp += character;

                // End of invalid character
                if ( character == ';' ) {

                    end = true;
                }

                // "&" must have been a part of either the value or the tag, save the xml as is (better safe than sorry)
                else if ( character == '<' || character == '>' ) {

                    // Reset values
                    start = end = false;
                    escapedXml += temp;
                    temp = "";
                }

            } else {

                escapedXml += character;
            }

            // Both start and end have been found (end will only be true if start is true), escape the character and add it
            // to the final output.
            if ( end ) {

                temp = StringEscapeUtils.escapeXml( temp );

                // Reset values
                start = end = false;
                escapedXml += temp;
                temp = "";
            }
        }

        return escapedXml;
    }

    /**
     * Returns a {@link List} of Strings, containing the values of a given tag. List is chosen as return type because it supports
     * situations where there are duplicate tags, as in the case with authors, and if the tag is missing, it should be easy to handle
     * this when using by checking that the List is not empty.
     *
     * @param tag The given tag to look for
     *
     * @return List of all the values ( as Strings ) for given tag
     */
    public List< String > getValuesFromTag( String tag ) {

        List< String > output = new ArrayList< String >();

        if ( doc != null ) {

            NodeList nodes = doc.getElementsByTagName( tag );

            for ( int i = 0; i < nodes.getLength(); i++ ) {

                output.add( nodes.item( i ).getTextContent() );
            }
        }

        return output;

    }

    /**
     * @return The name of the root tag. In this case, the publication type.
     */
    public String getRootTagName() {

        return doc.getDocumentElement().getTagName();
    }

    /**
     * @return The value for the attribute "type", for the root tag of the element
     */
    public String getRootAttributeKey() {

        return doc.getDocumentElement().getAttributes().getNamedItem( "key" ).getNodeValue();
    }

    /**
     * @return A list of all child nodes of root tag
     */
    public NodeList getChildElements() {

        return doc.getDocumentElement().getChildNodes();
    }
}
