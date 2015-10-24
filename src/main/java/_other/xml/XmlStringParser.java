package _other.xml;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class XmlStringParser {

    Document doc;

    public XmlStringParser( String xml ) {

        try {

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = null;
            builder = factory.newDocumentBuilder();

            InputSource input = new InputSource();
            input.setCharacterStream( new StringReader( xml ) );
            doc = builder.parse( input );

        } catch ( ParserConfigurationException e ) {

            e.printStackTrace();

        } catch ( SAXException e ) {

            e.printStackTrace();

        } catch ( IOException e ) {

            e.printStackTrace();
        }

    }

    public String getValueFromTag( String tag ) {

        NodeList nodes;

        try {

            if ( doc == null ) throw new Exception( "Doc is null!" );
            nodes = doc.getElementsByTagName( tag );
            if ( nodes == null ) throw new Exception( "Nodes is null!" );
            if ( nodes.getLength() != 0 ) return nodes.item( 0 ).getTextContent();

        } catch ( Exception e ) {
            e.printStackTrace();
        }

        return null;
    }

    public List< String > getValuesFromTag( String tag ) {

        NodeList nodes = doc.getElementsByTagName( tag );

        List< String > output = new ArrayList< String >();

        for ( int i = 0; i < nodes.getLength(); i++ ) {

            output.add( nodes.item( i ).getTextContent() );
        }

        return output;
    }

    public String getRootTagName() {

        return doc.getDocumentElement().getTagName();
    }

    public String getRootAttributeType() {

        return doc.getDocumentElement().getAttributes().getNamedItem( "type" ).getNodeValue();
    }

    public NodeList getChildElements() {

        return doc.getDocumentElement().getChildNodes();
    }
}
